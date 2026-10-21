import json
import random
import time

from confluent_kafka import Producer

BOOTSTRAP_SERVERS = "localhost:9092,localhost:9094,localhost:9096"
# BOOTSTRAP_SERVERS = "kafka2:9092"
TOPIC = "clickstream-events"

EVENT_TYPES = ["page_view", "search", "add_to_cart", "purchase_completed"]
USER_IDS = [
    "user-501",
    "user-502",
    "user-503",
    "user-504",
    "user-505",
    "user-506",
    "user-507",
]

PRODUCER_CONFIG = {
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "client.id": "event-tracking-service-demo",
}


def make_click_event(user_id: str) -> dict:
    return {
        "user_id": user_id,
        "event_type": random.choice(EVENT_TYPES),
        "event_time": time.time(),
    }


def delivery_report(err, msg):
    """Called once for every message to report success or failure."""
    if err is not None:
        print(f"Delivery failed for key={msg.key()}: {err}")
        return

    key = msg.key().decode("utf-8") if msg.key() else None
    print(
        f"Delivered to {msg.topic()} "
        f"partition {msg.partition()} "
        f"offset {msg.offset()} "
        f"key={key}"
    )


def main():
    producer = Producer(PRODUCER_CONFIG)

    for _ in range(20):
        user_id = random.choice(USER_IDS)
        event = make_click_event(user_id)

        producer.produce(
            topic=TOPIC,
            key=user_id.encode("utf-8"),
            value=json.dumps(event).encode("utf-8"),
            callback=delivery_report,
        )
        producer.poll(0)

    print("Flushing after all messages sent...")
    remaining = producer.flush(timeout=10)
    if remaining:
        print(f"Warning: {remaining} message(s) were not delivered")


if __name__ == "__main__":
    main()
