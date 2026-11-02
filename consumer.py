"""Consume checkout events, validate them, and write valid/bad records to Parquet."""
import json
import os
from datetime import datetime, timezone

import pandas as pd
from confluent_kafka import Consumer, KafkaException

BROKER = "localhost:19092"
TOPIC = "checkout-events"
GROUP_ID = "icestream-quality-checker"
DATA_DIR = "data"
VALID_FILE = os.path.join(DATA_DIR, "valid_transactions.parquet")
BAD_FILE = os.path.join(DATA_DIR, "quarantine.parquet")

EXPECTED_FIELDS = {
    "transaction_id", "event_time", "customer_id",
    "product", "amount", "tax_amount", "currency"
}

def validate_event(event):
    """Return (is_valid, reason)."""
    missing = EXPECTED_FIELDS - set(event.keys())
    unexpected = set(event.keys()) - EXPECTED_FIELDS

    if missing:
        return False, "Missing required field(s): " + ", ".join(sorted(missing))
    if unexpected:
        return False, "Schema drift: unexpected field(s): " + ", ".join(sorted(unexpected))
    if event["tax_amount"] is None:
        return False, "tax_amount is NULL"
    if not isinstance(event["amount"], (int, float)) or event["amount"] <= 0:
        return False, "amount must be a positive number"
    if not isinstance(event["tax_amount"], (int, float)) or event["tax_amount"] < 0:
        return False, "tax_amount must be a non-negative number"
    return True, "valid"

def append_parquet(record, path):
    os.makedirs(DATA_DIR, exist_ok=True)
    frame = pd.DataFrame([record])
    if os.path.exists(path):
        old = pd.read_parquet(path)
        frame = pd.concat([old, frame], ignore_index=True)
    frame.to_parquet(path, index=False)

def main():
    os.makedirs(DATA_DIR, exist_ok=True)
    consumer = Consumer({
        "bootstrap.servers": BROKER,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True
    })
    consumer.subscribe([TOPIC])
    print(f"Listening to '{TOPIC}'. Press Ctrl+C to stop.")
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Kafka error: {msg.error()}")
                continue
            try:
                event = json.loads(msg.value().decode("utf-8"))
                valid, reason = validate_event(event)
                event["quality_status"] = "VALID" if valid else "QUARANTINED"
                event["quality_reason"] = reason
                event["processed_at"] = datetime.now(timezone.utc).isoformat()
                if valid:
                    append_parquet(event, VALID_FILE)
                    print(f"VALID: {event.get('transaction_id')}")
                else:
                    append_parquet(event, BAD_FILE)
                    print(f"QUARANTINED: {event.get('transaction_id')} | {reason}")
            except (json.JSONDecodeError, UnicodeDecodeError, TypeError) as exc:
                bad = {
                    "raw_message": str(msg.value()),
                    "quality_status": "QUARANTINED",
                    "quality_reason": f"Message parsing error: {exc}",
                    "processed_at": datetime.now(timezone.utc).isoformat()
                }
                append_parquet(bad, BAD_FILE)
                print("QUARANTINED: message parsing error")
    except KeyboardInterrupt:
        print("\nStopping consumer...")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()
