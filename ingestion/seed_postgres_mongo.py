import os
import random
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from pymongo import MongoClient
from faker import Faker
import requests
from dotenv import load_dotenv

load_dotenv()  

fake = Faker()

PG_CONFIG = dict(
    host="localhost", port=5433,
    dbname="fraud_db", user="fraud_user", password=os.environ["POSTGRES_PASSWORD"],
)
MONGO_URI = "mongodb://localhost:27017"
PAYSIM_PATH = os.environ.get("PAYSIM_PATH", "./data/paysim.csv")
SEED_ROWS = int(os.environ.get("SEED_ROWS", "1500000"))


def seed_postgres():
    print("[postgres] membaca sample PaySim...")
    df = pd.read_csv(PAYSIM_PATH, nrows=SEED_ROWS)

    conn = psycopg2.connect(**PG_CONFIG)
    cur = conn.cursor()

    users = df["nameOrig"].unique()[:2000]
    merchants = [d for d in df["nameDest"].unique() if str(d).startswith("M")][:500]
    categories = ["retail", "electronics", "travel", "grocery", "digital_goods"]

    print(f"[postgres] insert {len(users)} users, {len(merchants)} merchants...")
    for u in users:
        cur.execute(
            "INSERT INTO users (external_id, name, account_balance) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
            (u, fake.name(), round(random.uniform(50, 5000), 2)),
        )
    for m in merchants:
        cur.execute(
            "INSERT INTO merchants (external_id, name, category, risk_score) VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING",
            (m, fake.company(), random.choice(categories), round(random.uniform(0, 1), 2)),
        )

    print(f"[postgres] insert {len(df)} transactions historis...")
    cur.execute("TRUNCATE TABLE transactions RESTART IDENTITY")  
    rows = list(zip(
        df["step"].astype(int), df["type"], df["amount"].astype(float),
        df["nameOrig"], df["nameDest"], df["isFraud"].astype(bool),
    ))
    execute_values(
        cur,
        """INSERT INTO transactions
           (step, type, amount, name_orig, name_dest, is_fraud)
           VALUES %s""",
        rows,
        page_size=5000,
    )

    conn.commit()
    cur.close()
    conn.close()
    print("[postgres] selesai.")


def geolocate_ip(ip):
    try:
        r = requests.get(f"http://ip-api.com/json/{ip}", timeout=3)
        data = r.json()
        return f'{data.get("city", "unknown")}, {data.get("country", "unknown")}'
    except Exception:
        return "unknown"


def seed_mongo(n_sessions=5000):
    print(f"[mongo] generate {n_sessions} device/session sintetis...")
    client = MongoClient(MONGO_URI)
    db = client["fraud_db"]
    col = db["device_sessions"]
    col.delete_many({})

    docs = []
    for _ in range(n_sessions):
        ip = fake.ipv4_public()
        docs.append({
            "session_id": fake.uuid4(),
            "user_external_id": f"C{random.randint(1000000000, 1999999999)}",
            "device_fingerprint": fake.sha256(),
            "ip_address": ip,
            "geolocation": geolocate_ip(ip) if random.random() < 0.02 else "unknown",  # panggil API sesekali saja
            "user_agent": fake.user_agent(),
            "timestamp": fake.date_time_this_year().isoformat(),
        })
        if len(docs) >= 500:
            col.insert_many(docs)
            docs = []
    if docs:
        col.insert_many(docs)

    print("[mongo] selesai.")


if __name__ == "__main__":
    seed_postgres()
    seed_mongo()
