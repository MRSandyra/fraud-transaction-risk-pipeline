import json
import os
import websocket
from kafka import KafkaProducer

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = "btc_large_tx"
WS_URL = "wss://ws.blockchain.info/inv"

MIN_BTC_THRESHOLD = float(os.environ.get("MIN_BTC_THRESHOLD", "5"))

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)


def total_btc_out(tx):
    """Jumlah total BTC yang keluar dari seluruh output transaksi (dalam satoshi -> BTC)."""
    return sum(o.get("value", 0) for o in tx.get("out", [])) / 1e8


def on_open(ws):
    print("[btc-producer] terhubung, subscribe ke unconfirmed transactions...")
    ws.send(json.dumps({"op": "unconfirmed_sub"}))


def on_message(ws, message):
    data = json.loads(message)
    if data.get("op") != "utx":
        return
    tx = data["x"]
    btc_value = total_btc_out(tx)
    if btc_value >= MIN_BTC_THRESHOLD:
        event = {
            "tx_hash": tx.get("hash"),
            "btc_value": btc_value,
            "n_inputs": len(tx.get("inputs", [])),
            "n_outputs": len(tx.get("out", [])),
            "time": tx.get("time"),
        }
        producer.send(TOPIC, value=event)
        print(f"[btc-producer] large tx terdeteksi: {btc_value:.2f} BTC")


def on_error(ws, error):
    print(f"[btc-producer] error: {error}")


if __name__ == "__main__":
    ws_app = websocket.WebSocketApp(
        WS_URL, on_open=on_open, on_message=on_message, on_error=on_error
    )
    ws_app.run_forever()
