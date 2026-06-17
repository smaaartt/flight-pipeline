import os
import json
import psycopg2
from kafka import KafkaConsumer
from datetime import datetime
import time

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC           = "flight-positions"

DB_CONFIG = {
    "host":     os.getenv("DB_HOST", "localhost"),
    "port":     os.getenv("DB_PORT", 5432),
    "dbname":   os.getenv("DB_NAME", "flight_db"),
    "user":     os.getenv("DB_USER", "flight"),
    "password": os.getenv("DB_PASSWORD", "flight"),
}

def get_consumer():
    while True:
        try:
            consumer = KafkaConsumer(
                TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                auto_offset_reset="latest",
                group_id="flight-consumer-group"
            )
            print("[Consumer] Connecté à Kafka, en écoute...")
            return consumer
        except Exception as e:
            print(f"[Consumer] Kafka pas prêt, retry dans 5s... ({e})")
            time.sleep(5)

def get_db():
    while True:
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            print("[Consumer] Connecté à PostgreSQL")
            return conn
        except Exception as e:
            print(f"[Consumer] DB pas prête, retry dans 5s... ({e})")
            time.sleep(5)

def insert_flight(cur, msg):
    cur.execute("""
        INSERT INTO streaming_flights
            (icao24, callsign, origin_country, longitude, latitude,
             altitude, on_ground, velocity, received_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        msg.get("icao24"),
        msg.get("callsign"),
        msg.get("origin_country"),
        msg.get("longitude"),
        msg.get("latitude"),
        msg.get("altitude"),
        msg.get("on_ground"),
        msg.get("velocity"),
        datetime.now()
    ))

if __name__ == "__main__":
    consumer = get_consumer()
    conn     = get_db()
    cur      = conn.cursor()
    count    = 0

    for message in consumer:
        try:
            insert_flight(cur, message.value)
            conn.commit()
            count += 1
            if count % 100 == 0:
                print(f"[{datetime.now()}] {count} messages consommés")
        except Exception as e:
            print(f"[Consumer] Erreur insertion : {e}")
            conn.rollback()