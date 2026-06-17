import os
import time
import json
import psycopg2
from kafka import KafkaProducer
from datetime import datetime

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC           = "flight-positions"

DB_CONFIG = {
    "host":     os.getenv("DB_HOST", "postgres"),
    "port":     int(os.getenv("DB_PORT", 5432)),
    "dbname":   os.getenv("DB_NAME", "flight_db"),
    "user":     os.getenv("DB_USER", "flight"),
    "password": os.getenv("DB_PASSWORD", "flight"),
}

def get_producer():
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8")
            )
            print("[Producer] Connecté à Kafka")
            return producer
        except Exception as e:
            print(f"[Producer] Kafka pas prêt, retry dans 5s... ({e})")
            time.sleep(5)

def fetch_from_db():
    """Lit les vols récents depuis PostgreSQL au lieu d'appeler l'API"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur  = conn.cursor()
        cur.execute("""
            SELECT icao24, callsign, origin_country, longitude, latitude,
                   altitude, on_ground, velocity, ingested_at
            FROM raw_flights
            WHERE ingested_at > NOW() - INTERVAL '10 minutes'
            ORDER BY ingested_at DESC
            LIMIT 500
        """)
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return rows
    except Exception as e:
        print(f"[Producer] Erreur DB : {e}")
        return []

def produce(producer, rows):
    count = 0
    for row in rows:
        message = {
            "icao24":         row[0],
            "callsign":       row[1],
            "origin_country": row[2],
            "longitude":      row[3],
            "latitude":       row[4],
            "altitude":       row[5],
            "on_ground":      row[6],
            "velocity":       row[7],
            "timestamp":      row[8].isoformat() if row[8] else None
        }
        producer.send(TOPIC, value=message)
        count += 1

    producer.flush()
    print(f"[{datetime.now()}] {count} messages envoyés dans Kafka depuis DB")

if __name__ == "__main__":
    # Attendre que l'ETL ait chargé des données en premier
    print("[Producer] Attente initiale de 90s pour laisser l'ETL charger...")
    time.sleep(90)

    producer = get_producer()

    while True:
        rows = fetch_from_db()
        if rows:
            produce(producer, rows)
        else:
            print("[Producer] Pas encore de données en base, attente...")
        time.sleep(60)