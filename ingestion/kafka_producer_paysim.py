import csv
import json
import os
import time
from kafka import KafkaProducer

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = "transactions_stream"
PAYSIM_PATH = os.environ.get("PAYSIM_PATH", "./data/paysim.csv")

SPEEDUP_SECONDS = float(os.environ.get("SPEEDUP_SECONDS", "2"))
INTRA_STEP_DELAY = float(os.environ.get("INTRA_STEP_DELAY", "0.05"))
MAX_ROWS = int(os.environ.get("MAX_ROWS", "50000"))


def make_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        linger_ms=10,
    )


def replay():
    producer = make_producer()
    current_step = None
    sent = 0

    with open(PAYSIM_PATH, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if MAX_ROWS and sent >= MAX_ROWS:
                break

            step = int(row["step"])
            if current_step is None:
                current_step = step
            elif step != current_step:
                time.sleep(SPEEDUP_SECONDS)
                current_step = step

            event = {
                "step": step,
                "type": row["type"],
                "amount": float(row["amount"]),
                "name_orig": row["nameOrig"],
                "oldbalance_orig": float(row["oldbalanceOrg"]),
                "newbalance_orig": float(row["newbalanceOrig"]),
                "name_dest": row["nameDest"],
                "oldbalance_dest": float(row["oldbalanceDest"]),
                "newbalance_dest": float(row["newbalanceDest"]),
                "is_fraud": bool(int(row["isFraud"])),
                "event_time": time.time(),  
            }

            producer.send(TOPIC, value=event)
            sent += 1
            if sent % 500 == 0:
                print(f"[producer] terkirim {sent} event, step terakhir={current_step}")

            time.sleep(INTRA_STEP_DELAY)

    producer.flush()
    print(f"[producer] selesai. total event terkirim: {sent}")


if __name__ == "__main__":
    replay()
