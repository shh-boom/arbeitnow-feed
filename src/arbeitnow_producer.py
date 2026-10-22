import time
import random
import json
from confluent_kafka import Producer


# Step 1: Connect to the Kafka Cluster – Bootstrap, Client id, Other Configs
BOOTSTRAP_SERVERS = "localhost:9092,localhost:9094,localhost:9095"
TOPIC = "arbeitnow-events"

producer_config = {
    "bootstrap.servers" : BOOTSTRAP_SERVERS,
    "client.id" : "event-tracking-service-demo"
}

# Step 2: Create the Producer API 
producer = Producer(producer_config)

# EVENT_TYPES = ["page_view", "search", "add_to_cart", "purchase_completed"]


# Step 3: Define the Message — Key and Value – Serialized to binary

# def make_click_event(user_id: str) -> dict:
#     return {
#         "user_id": user_id,
#         "event_type": random.choice(EVENT_TYPES),
#         "event_time": time.time(),
#     }


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


def produce_record(record: dict):
    # user_ids = ["user-501", "user-502", "user-503", "user-504", "user-505", "user-506", "user-507"]

    # for _ in range(written):
        # user_id = random.choice(user_ids)
        # event = make_click_event(user_id)

        # Step 4: Send/Produce the Message
        producer.produce(
            topic=TOPIC,
            key=record["slug"].encode("utf-8"),
            value= json.dumps(record).encode("utf-8"),
            callback=delivery_report
        )

        # print(f"Pooling after event is sent to Kafka {user_id}:{event}")
        # Step 5: Know When a Message Actually Received – Callback 
        producer.poll(0)
        # time.sleep(1)

    # Step 6: Make Sure Nothing Is Left Behind - Flush
        print("Flushing after all messages sent...")
        producer.flush()

# if __name__ == "__main__":
#     produce_record()
