import json
import time
import uuid
from datetime import datetime, timezone, timedelta
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

BOOTSTRAP_SERVERS = "kafka:9092"

EVENT_INTERVAL_SEC = 2          # delay between events
CYCLE_SLEEP_SEC = 10            # pause after one full cycle
RUN_FOREVER = True           # set False to stop after N cycles
MAX_CYCLES = 5                  # used only if RUN_FOREVER = False


def create_producer():
    while True:
        try:
            print("Trying to connect to Kafka...")
            producer = KafkaProducer(
                bootstrap_servers=BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
                retries=5
            )
            print("Connected to Kafka ✅")
            return producer
        except NoBrokersAvailable:
            print("Kafka not ready, retrying...")
            time.sleep(5)


def now_iso():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def mutate_event(event, topic):
    e = event.copy()

    # Generate unique IDs
    e["event_id"] = f"{topic[:3]}-{uuid.uuid4()}"
    e["trace_id"] = f"trace-{uuid.uuid4()}"

    # Update timestamps safely
    e["event_time"] = now_iso()

    if "processed_at" in e:
        e["processed_at"] = now_iso()

    if "created_at" in e:
        e["created_at"] = now_iso()

    if "registered_at" in e:
        e["registered_at"] = now_iso()

    return e


def main():
    producer = create_producer()

    with open("input.json") as f:
        base_data = json.load(f)

    cycle = 0

    while RUN_FOREVER or cycle < MAX_CYCLES:
        cycle += 1
        print(f"\n--- PRODUCER CYCLE {cycle} ---")

        for topic, events in base_data.items():
            for event in events:
                mutated_event = mutate_event(event, topic)
                producer.send(topic, value=mutated_event)
                print(f"Sent {mutated_event['event_id']} → {topic}")
                time.sleep(EVENT_INTERVAL_SEC)

        producer.flush()
        print("Cycle completed, sleeping...\n")
        time.sleep(CYCLE_SLEEP_SEC)

    producer.close()
    print("Producer stopped gracefully 🛑")


if __name__ == "__main__":
    main()
