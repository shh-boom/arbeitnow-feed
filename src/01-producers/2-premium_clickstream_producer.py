import time
import random
import json
import zlib
from confluent_kafka import Producer

# Step 1: Connect to the Kafka Cluster – Bootstrap, Client id, Other Configs
BOOTSTRAP_SERVERS = "localhost:9092,localhost:9094,localhost:9095"
TOPIC = "clickstream-events"

producer_config = {
    "bootstrap.servers" : BOOTSTRAP_SERVERS,
    "client.id" : "event-tracking-service-demo"
}

# Step 2: Create the Producer API 
producer = Producer(producer_config)

EVENT_TYPES = ["page_view", "search", "add_to_cart", "purchase_completed"]

def make_click_event(user_id: str, tier: str) -> dict:
    return {
        "user_id": user_id,
        "tier": tier,
        "event_type": random.choice(EVENT_TYPES),
        "event_time": time.time(),
    }

def delivery_report(err, msg):
    """Called once for every message, to report success or failure."""
    if err is not None:
        print(f"Delivery failed for key={msg.key()}: {err}")
    else:
        print(
            f"Delivered to {msg.topic()} "
            f"[partition {msg.partition()}] "
            f"offset {msg.offset()} "
            f"key={msg.key().decode('utf-8')}"
        )


def get_partition_count(producer: Producer, topic: str) -> int:
    cluster_metadata = producer.list_topics(topic, timeout=10)
    topic_metadata = cluster_metadata.topics[topic]

    if topic_metadata.error is not None:
        raise RuntimeError(f"Could not fetch metadata for '{topic}': {topic_metadata.error}")

    return len(topic_metadata.partitions)


def decide_partition(user_id: str, tier: str) -> int:
    if tier == 'premium':
        return 0

    remaining_partitions = get_partition_count(producer, TOPIC) - 1 
    partition_no = zlib.crc32(user_id.encode("utf-8")) % remaining_partitions
    return partition_no + 1

def main():
    # A realistic mix: most users are standard, some are premium subscribers.
    users = [
        ("user-501", "standard"),
        ("user-502", "premium"),
        ("user-503", "standard"),
        ("user-504", "standard"),
        ("user-505", "premium"),
        ("user-506", "standard"),
        ("user-507", "standard")
    ]

    for _ in range(30):
        user_id, tier = random.choice(users)
        event = make_click_event(user_id, tier)
        partition_no = decide_partition(user_id, tier)    

        """Premium users' events always go to the dedicated low-latency
            partition 0. Everyone else is spread across the remaining
            partitions using a simple, consistent hash — the same user ID
            always lands on the same non-premium partition, it just never
            lands on Partition 0.
        """
        producer.produce(
            topic=TOPIC,
            key=user_id.encode("utf-8"),
            value=json.dumps(event).encode("utf-8"),
            partition= partition_no,
            callback=delivery_report,
        )

        producer.poll(0)
        time.sleep(1)

    producer.flush()
    print("All events sent.")

if __name__ == "__main__":
    main()
