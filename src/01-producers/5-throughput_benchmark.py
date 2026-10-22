import sys
import json
import time
from confluent_kafka import Producer

BOOTSTRAP_SERVERS = "localhost:9092,localhost:9094,localhost:9095"
TOPIC = "clickstream-benchmark"
NUM_MESSAGES = 20000

CONFIGS = {
    # librdkafka's own defaults, made explicit so you can see exactly
    # what you're comparing against.
    "baseline": {
        "acks": 1,
        "linger.ms": 5,
        "batch.size": 1000000,
        "compression.type": "none",
    },
    # Optimized for maximum messages/second: a longer linger to build
    # bigger batches, plus compression to shrink what's on the wire.
    "throughput": {
        "acks": 1,
        "linger.ms": 50,
        "batch.size": 1000000,
        "compression.type": "lz4",
    },
    # Maximum safety: wait for every in-sync replica to confirm
    # before considering a message delivered.
    "durability": {
        "acks": "all",
        "linger.ms": 5,
        "batch.size": 1000000,
        "compression.type": "lz4",
    },
}


def main():
    if len(sys.argv) != 2 or sys.argv[1] not in CONFIGS:
        print("Usage: python3 5-throughput_benchmark.py [baseline|throughput|durability]")
        sys.exit(1)

    scenario = sys.argv[1]
    config = {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "client.id": f"benchmark-{scenario}",
        **CONFIGS[scenario],
    }

    producer = Producer(config)

    print(f"Running scenario: {scenario}")
    print(f"Config: {CONFIGS[scenario]}")
    print(f"Sending {NUM_MESSAGES} messages...")

    start = time.time()

    for i in range(NUM_MESSAGES):
        event = {
            "user_id": f"user-{i % 500}",
            "event_type": "page_view",
            "event_time": time.time(),
        }
        producer.produce(
            topic=TOPIC,
            key=str(i % 500).encode("utf-8"),
            value=json.dumps(event).encode("utf-8"),
        )
        # Poll periodically to keep delivery callbacks and internal
        # librdkafka housekeeping moving, without slowing the loop
        # down by polling after every single message.
        if i % 1000 == 0:
            producer.poll(0)

    producer.flush()
    elapsed = time.time() - start
    throughput = NUM_MESSAGES / elapsed

    print(f"\nDone in {elapsed:.2f} seconds")
    print(f"Throughput: {throughput:.0f} messages/second")


if __name__ == "__main__":
    main()
