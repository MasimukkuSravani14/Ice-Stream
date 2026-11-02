"""Generate mock e-commerce checkout events and publish them to Redpanda/Kafka."""
import json
import random
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer

BROKER = "localhost:19092"
TOPIC = "checkout-events"

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(f"Sent event to {msg.topic()} [{msg.partition()}]")

def make_transaction():
    event = {
        "transaction_id": str(uuid.uuid4())[:8],
        "event_time": datetime.now(timezone.utc).isoformat(),
        "customer_id": f"C{random.randint(1000, 9999)}",
        "product": random.choice(["Laptop", "Phone", "Headphones", "Keyboard"]),
        "amount": round(random.uniform(100, 5000), 2),
        "tax_amount": round(random.uniform(5, 500), 2),
        "currency": "INR"
    }

    # Intentionally inject data-quality problems for demonstration.
    chance = random.random()
    if chance < 0.10:
        event["tax_amount"] = None            # Null value
    elif chance < 0.15:
        event["unexpected_discount_code"] = "NEW_SCHEMA"  # Schema drift
    elif chance < 0.18:
        event["amount"] = -50                 # Invalid business value

    return event

def main():
    producer = Producer({"bootstrap.servers": BROKER})
    print(f"Producing mock checkout events to topic '{TOPIC}'. Press Ctrl+C to stop.")
    try:
        while True:
            event = make_transaction()
            producer.produce(
                TOPIC,
                key=event["transaction_id"],
                value=json.dumps(event),
                callback=delivery_report
            )
            producer.poll(0)
            time.sleep(0.5)
    except KeyboardInterrupt:
        print("\nStopping producer...")
    finally:
        producer.flush()

if __name__ == "__main__":
    main()
