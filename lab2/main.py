import json
import logging
from datetime import datetime
from typing import Any, Dict

from confluent_kafka import Consumer, KafkaError
from sqlalchemy import (
    Column,
    DateTime,
    Integer,
    MetaData,
    String,
    Table,
    create_engine,
    text,
)
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.engine import Engine
from sqlalchemy.types import TypeEngine

from config import PG_DB, PG_HOST, PG_PASSWORD, PG_PORT, PG_USER, env_str

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


KAFKA_BOOTSTRAP_SERVERS = env_str("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = env_str("KAFKA_TOPIC", "user-events")
KAFKA_GROUP_ID = env_str("KAFKA_GROUP_ID", "kafka-processor-group")

def _build_connection_url() -> str:
    return (
        f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}"
        f"@{PG_HOST}:{PG_PORT}/{PG_DB}"
    )


engine: Engine = create_engine(_build_connection_url())
metadata = MetaData()


def _infer_sql_type(value: Any) -> TypeEngine:

    if isinstance(value, bool):
        return String(10)
    if isinstance(value, int):
        return Integer()
    if isinstance(value, float):
        return String(50)
    if isinstance(value, datetime):
        return DateTime()
    
    if isinstance(value, str):
        if "T" in value or ("-" in value and ":" in value):
            try:
                datetime.fromisoformat(value.replace("Z", "+00:00"))
                return DateTime()
            except (ValueError, AttributeError):
                pass
    return String(500)


def _create_table_from_schema(
    table_name: str, schema: Dict[str, Any], is_history: bool = True
) -> Table:

    columns = [
        Column("id", Integer, primary_key=True, autoincrement=True),
    ]

    for key, value in schema.items():
        if key == "timestamp":
            columns.append(Column("timestamp", DateTime, nullable=False))
        else:
            sql_type = _infer_sql_type(value)
            columns.append(Column(key, sql_type, nullable=True))

    return Table(table_name, metadata, *columns)


def _ensure_table_exists(
    table_name: str, schema: Dict[str, Any], is_history: bool = True
) -> Table:
 
    if table_name in metadata.tables:
        return metadata.tables[table_name]

    table = _create_table_from_schema(table_name, schema, is_history)
    metadata.create_all(engine, tables=[table])


    if not is_history:
        key_field = None
        for key in ["user_id", "id", "key"]:
            if key in schema:
                key_field = key
                break

        if key_field:
            with engine.begin() as conn:
                index_name = f"{table_name}_{key_field}_idx"
                check_index = text(
                    f"""
                    SELECT EXISTS (
                        SELECT 1 FROM pg_indexes 
                        WHERE indexname = '{index_name}'
                    )
                """
                )
                exists = conn.execute(check_index).scalar()
                if not exists:
                    create_index = text(
                        f"""
                        CREATE UNIQUE INDEX IF NOT EXISTS {index_name}
                        ON {table_name} ({key_field})
                    """
                    )
                    conn.execute(create_index)
                    logger.info(f"Created unique index {index_name} on {table_name}")

    logger.info(f"Table {table_name} ensured")
    return table


def _save_to_history(table_name: str, data: Dict[str, Any]) -> None:

    history_table_name = f"{table_name}_history"
    table = _ensure_table_exists(history_table_name, data, is_history=True)


    insert_data = data.copy()
    if "timestamp" in insert_data and isinstance(insert_data["timestamp"], str):
        try:
            insert_data["timestamp"] = datetime.fromisoformat(
                insert_data["timestamp"].replace("Z", "+00:00")
            )
        except Exception:
            insert_data["timestamp"] = datetime.utcnow()

    with engine.begin() as conn:
        stmt = table.insert().values(**insert_data)
        conn.execute(stmt)
    logger.info(f"Saved to history: {history_table_name}")


def _save_to_latest(table_name: str, data: Dict[str, Any]) -> None:

    latest_table_name = f"{table_name}_latest"
    table = _ensure_table_exists(latest_table_name, data, is_history=False)


    key_field = None
    for key in ["user_id", "id", "key"]:
        if key in data:
            key_field = key
            break

    if not key_field:
        logger.warning("No key field found, skipping latest update")
        return

    key_value = data[key_field]

    insert_data = data.copy()
    if "timestamp" in insert_data and isinstance(insert_data["timestamp"], str):
        try:
            insert_data["timestamp"] = datetime.fromisoformat(
                insert_data["timestamp"].replace("Z", "+00:00")
            )
        except Exception:
            insert_data["timestamp"] = datetime.utcnow()

    with engine.begin() as conn:

        stmt = table.select().where(table.c[key_field] == key_value)
        existing = conn.execute(stmt).mappings().first()

        new_timestamp = insert_data.get("timestamp")


        if existing and new_timestamp:
            existing_timestamp = existing.get("timestamp")
            if existing_timestamp and new_timestamp < existing_timestamp:
                logger.info(
                    f"Skipped update for {key_field}={key_value}: "
                    f"existing={existing_timestamp}, new={new_timestamp}"
                )
                return

        insert_data_no_id = {k: v for k, v in insert_data.items() if k != "id"}

        upsert_stmt = pg_insert(table).values(**insert_data_no_id)

        update_dict = {
            col.name: upsert_stmt.excluded[col.name]
            for col in table.columns
            if col.name not in [key_field, "id"]
        }

        upsert_stmt = upsert_stmt.on_conflict_do_update(
            index_elements=[key_field], set_=update_dict
        )

        conn.execute(upsert_stmt)
        logger.info(
            f"Upserted latest for {key_field}={key_value} in {latest_table_name}"
        )


def process_message(data: Dict[str, Any], topic: str) -> None:
    try:
        table_name = topic.replace("-", "_")

      
        _save_to_history(table_name, data)

        _save_to_latest(table_name, data)

        logger.info(f"Processed message from topic {topic}: {data}")
    except Exception as exc:
        logger.error(f"Error processing message {data}: {exc}", exc_info=True)


def main() -> None:

    consumer_config = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": KAFKA_GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "max.poll.interval.ms": 300000,
        "session.timeout.ms": 30000,
        "heartbeat.interval.ms": 10000,
    }

    consumer = Consumer(consumer_config)
    consumer.subscribe([KAFKA_TOPIC])

    logger.info(
        f"Starting Kafka processor. Topic: {KAFKA_TOPIC}, "
        f"Bootstrap servers: {KAFKA_BOOTSTRAP_SERVERS}"
    )

    try:
        while True:
            messages = consumer.consume(num_messages=100, timeout=1.0)
            if not messages:
                continue

            for msg in messages:
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    logger.error(f"Kafka error: {msg.error()}")
                    continue

                try:
                    value = json.loads(msg.value().decode("utf-8"))
                    process_message(value, msg.topic())
                except Exception as exc:
                    logger.error(f"Error processing message: {exc}", exc_info=True)
                    continue

            consumer.commit()
            logger.debug("Committed offsets")

    except KeyboardInterrupt:
        logger.info("Shutting down Kafka processor...")
    finally:
        consumer.close()
        logger.info("Kafka processor stopped")


if __name__ == "__main__":
    main()
