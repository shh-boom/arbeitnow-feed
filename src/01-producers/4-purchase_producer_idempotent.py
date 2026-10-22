import json
import time
from confluent_kafka import Producer

BOOTSTRAP_SERVERS = "localhost:9092,localhost:9094,localhost:9095"
TOPIC = "purchase-events-idempotent"

producer_config = {
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "client.id": "purchase-producer-idempotent",
    "enable.idempotence": True,
    # Enabling idempotence automatically adjusts several other settings
    # for you: acks becomes "all", retries becomes very high, and
    # max.in.flight.requests.per.connection is capped at 5. You don't
    # need to set any of these yourself.
    "message.timeout.ms": 300000,
}

producer = Producer(producer_config)


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed for purchase_id={msg.key().decode('utf-8')}: {err}")


def main():

    for purchase_id in range(100000):
        event = {
            "purchase_id": purchase_id,
            "user_id": "user-501",
            "amount": 49.99,
            "purchased_at": time.time(),
        }

        producer.produce(
            topic=TOPIC,
            key=str(purchase_id).encode("utf-8"),
            value=json.dumps(event).encode("utf-8"),
            callback=delivery_report,
        )

        producer.poll(0)
        time.sleep(0.01)

    producer.flush()
    print("All purchase events sent (or failed) — check the count in the topic.")


if __name__ == "__main__":
    main()
