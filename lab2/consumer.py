import json

from confluent_kafka import Consumer, KafkaError

from config import env_str

KAFKA_BOOTSTRAP_SERVERS = env_str("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = env_str("KAFKA_TOPIC", "user-events")
KAFKA_GROUP_ID = env_str("KAFKA_GROUP_ID", "python-consumer")

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


def process_message(value: dict) -> None:
    print(f"сообщение: {value}")


if __name__ == "__main__":
    try:
        while True:
            messages = consumer.consume(num_messages=100, timeout=1.0)
            if not messages:
                continue

            for msg in messages:
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    print(f"Ошибка: {msg.error()}")
                    continue

                try:
                    value = json.loads(msg.value().decode("utf-8"))
                    process_message(value)
                except Exception as exc:  
                    print(f"Ошибка: {exc}")
                    continue

            consumer.commit()

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

