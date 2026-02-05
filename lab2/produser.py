import json
from datetime import datetime

from confluent_kafka import Producer

from config import env_str

KAFKA_BOOTSTRAP_SERVERS = env_str("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_CLIENT_ID = env_str("KAFKA_CLIENT_ID", "python-producer")
KAFKA_TOPIC = env_str("KAFKA_TOPIC", "user-events")

producer_config = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "client.id": KAFKA_CLIENT_ID,
}

producer = Producer(producer_config)


def delivery_report(err, msg):

    if err is not None:
        print(f"Ошибка доставки сообщения: {err}")
    else:
        print(
            f"Сообщение доставлено в {msg.topic()} "
            f"[{msg.partition()}] offset={msg.offset()}"
        )


def send_event(user_id: int, email: str, action: str) -> None:

    payload = {
        "user_id": user_id,
        "email": email,
        "action": action,
        "timestamp": datetime.utcnow().isoformat(),
    }
    producer.produce(
        topic=KAFKA_TOPIC,
        key=str(user_id),
        value=json.dumps(payload),
        callback=delivery_report,
    )
    producer.flush()


if __name__ == "__main__":

    send_event(1, "ivanov@mail.com", "created")
    send_event(1, "ivanov_ii@mail.com", "updated")
    send_event(2, "petrov@mail.com", "created")
