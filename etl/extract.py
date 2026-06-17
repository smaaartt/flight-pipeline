import os
import time
import requests
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime

DB_CONFIG = {
    "host":     os.getenv("DB_HOST", "localhost"),
    "port":     os.getenv("DB_PORT", 5432),
    "dbname":   os.getenv("DB_NAME", "flight_db"),
    "user":     os.getenv("DB_USER", "flight"),
    "password": os.getenv("DB_PASSWORD", "flight"),
}

OPENSKY_URL  = "https://opensky-network.org/api/states/all"
OPENSKY_USER = os.getenv("OPENSKY_USER", "")
OPENSKY_PASS = os.getenv("OPENSKY_PASS", "")

AVIATIONSTACK_URL = "http://api.aviationstack.com/v1/flights"
AVIATIONSTACK_KEY = os.getenv("AVIATIONSTACK_KEY", "")

# Bounding box Europe
PARAMS = {
    "lamin": 35.0, "lomin": -10.0,
    "lamax": 60.0, "lomax": 30.0,
}

# Aéroports européens majeurs à surveiller
AIRPORTS = ["CDG", "LHR", "FRA", "AMS", "MAD", "FCO", "BCN", "MUC"]

# ─── OpenSky ──────────────────────────────────────────────────────────────
def fetch_opensky():
    print(f"[{datetime.now()}] Appel OpenSky API...")
    try:
        response = requests.get(
            OPENSKY_URL, params=PARAMS,
            auth=(OPENSKY_USER, OPENSKY_PASS),
            timeout=15
        )
        response.raise_for_status()
        states = response.json().get("states", [])
        print(f"  → {len(states)} vols récupérés")
        return states
    except Exception as e:
        print(f"  ✗ Erreur OpenSky : {e}")
        return []

def transform_opensky(states):
    rows = []
    for s in states:
        if s[5] is None or s[6] is None:
            continue
        rows.append((
            s[0], s[1].strip() if s[1] else None, s[2],
            s[5], s[6], s[7], s[8], s[9]
        ))
    print(f"  → {len(rows)} vols valides")
    return rows

def load_opensky(rows):
    if not rows:
        return
    conn = psycopg2.connect(**DB_CONFIG)
    cur  = conn.cursor()
    execute_values(cur, """
        INSERT INTO raw_flights
            (icao24, callsign, origin_country, longitude, latitude,
             altitude, on_ground, velocity)
        VALUES %s
        ON CONFLICT DO NOTHING
    """, rows)
    conn.commit()
    cur.close()
    conn.close()
    print(f"  → {len(rows)} vols chargés en base")

# ─── AviationStack ────────────────────────────────────────────────────────
def fetch_aviationstack():
    print(f"[{datetime.now()}] Appel AviationStack API...")
    all_rows = []
    for airport in AIRPORTS:
        try:
            response = requests.get(AVIATIONSTACK_URL, params={
                "access_key": AVIATIONSTACK_KEY,
                "dep_iata":   airport,
                "limit":      10
            }, timeout=15)
            response.raise_for_status()
            flights = response.json().get("data", [])

            for f in flights:
                dep = f.get("departure", {})
                arr = f.get("arrival", {})

                scheduled_dep = dep.get("scheduled")
                actual_dep    = dep.get("actual")
                scheduled_arr = arr.get("scheduled")
                actual_arr    = arr.get("actual")
                delay         = dep.get("delay") or 0

                all_rows.append((
                    f.get("flight", {}).get("iata"),
                    f.get("airline", {}).get("name"),
                    dep.get("iata"),
                    arr.get("iata"),
                    scheduled_dep,
                    actual_dep,
                    scheduled_arr,
                    actual_arr,
                    f.get("flight_status"),
                    int(delay)
                ))
            print(f"  → {airport} : {len(flights)} vols récupérés")
            time.sleep(2)  # petit délai entre chaque aéroport

        except Exception as e:
            print(f"  ✗ Erreur AviationStack ({airport}) : {e}")

    return all_rows

def load_aviationstack(rows):
    if not rows:
        return
    conn = psycopg2.connect(**DB_CONFIG)
    cur  = conn.cursor()
    execute_values(cur, """
        INSERT INTO raw_flight_status
            (flight_number, airline, departure_airport, arrival_airport,
             scheduled_departure, actual_departure, scheduled_arrival,
             actual_arrival, status, delay_minutes)
        VALUES %s
    """, rows)
    conn.commit()
    cur.close()
    conn.close()
    print(f"  → {len(rows)} statuts de vols chargés en base")

# ─── Main ─────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    call_count = 0
    while True:
        # OpenSky toutes les itérations (120s)
        states = fetch_opensky()
        rows   = transform_opensky(states)
        load_opensky(rows)

        # AviationStack toutes les 30 itérations (~1h)
        call_count += 1
        if call_count == 1 or call_count % 30 == 0:
            av_rows = fetch_aviationstack()
            load_aviationstack(av_rows)

        print(f"  Prochain appel OpenSky dans 120s...\n")
        time.sleep(120)