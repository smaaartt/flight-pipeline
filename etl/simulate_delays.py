import os
import random
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timedelta

DB_CONFIG = {
    "host":     os.getenv("DB_HOST", "postgres"),
    "port":     int(os.getenv("DB_PORT", 5432)),
    "dbname":   os.getenv("DB_NAME", "flight_db"),
    "user":     os.getenv("DB_USER", "flight"),
    "password": os.getenv("DB_PASSWORD", "flight"),
}

# Aéroports avec leurs biais de retard réalistes
AIRPORT_DELAY_BIAS = {
    "LHR": 18,   # Heathrow — très congestionné
    "CDG": 15,   # Paris CDG — souvent en retard
    "FCO": 14,   # Rome — retards fréquents
    "BCN": 12,   # Barcelone
    "MAD": 10,   # Madrid
    "FRA": 8,    # Frankfurt — plutôt ponctuel
    "AMS": 6,    # Amsterdam — très ponctuel
    "MUC": 5,    # Munich — le plus ponctuel
}

# Compagnies aériennes européennes réalistes
AIRLINES = [
    "Ryanair", "EasyJet", "Lufthansa", "Air France",
    "British Airways", "Iberia", "Alitalia", "KLM",
    "Vueling", "Wizz Air", "Turkish Airlines", "Swiss"
]

# Biais de retard par compagnie
AIRLINE_DELAY_BIAS = {
    "Ryanair":         5,
    "EasyJet":         8,
    "Wizz Air":        10,
    "Alitalia":        15,
    "Air France":      12,
    "British Airways": 10,
    "Iberia":          8,
    "Lufthansa":       6,
    "KLM":             5,
    "Swiss":           4,
    "Vueling":         9,
    "Turkish Airlines": 11,
}

def generate_delay(airport_bias, airline_bias):
    """Génère un retard réaliste selon des probabilités statistiques."""
    base = airport_bias + airline_bias
    rand = random.random()

    if rand < 0.60:
        # Vol à l'heure
        return random.randint(0, 5), "scheduled"
    elif rand < 0.80:
        # Petit retard
        return random.randint(5, 30) + base // 3, "active"
    elif rand < 0.93:
        # Retard moyen
        return random.randint(30, 120) + base, "delayed"
    elif rand < 0.98:
        # Grand retard
        return random.randint(120, 300) + base, "delayed"
    else:
        # Annulé
        return 0, "cancelled"

def simulate_delays():
    conn = psycopg2.connect(**DB_CONFIG)
    cur  = conn.cursor()

    # Récupère les vols réels des dernières 2 heures
    cur.execute("""
        SELECT DISTINCT ON (callsign)
            callsign, origin_country
        FROM raw_flights
        WHERE callsign IS NOT NULL
          AND ingested_at > NOW() - INTERVAL '2 hours'
        LIMIT 300
    """)
    flights = cur.fetchall()

    if not flights:
        print("[Simulator] Pas encore de vols en base, attente...")
        cur.close()
        conn.close()
        return 0

    rows = []
    airports = list(AIRPORT_DELAY_BIAS.keys())

    for callsign, origin_country in flights:
        dep_airport  = random.choice(airports)
        arr_airport  = random.choice([a for a in airports if a != dep_airport])
        airline      = random.choice(AIRLINES)

        airport_bias = AIRPORT_DELAY_BIAS.get(dep_airport, 8)
        airline_bias = AIRLINE_DELAY_BIAS.get(airline, 8)

        delay, status = generate_delay(airport_bias, airline_bias)

        # Horaires simulés réalistes
        scheduled_dep = datetime.now() - timedelta(
            hours=random.randint(0, 6),
            minutes=random.randint(0, 59)
        )
        actual_dep = scheduled_dep + timedelta(minutes=delay)
        flight_time = timedelta(hours=random.randint(1, 4))
        scheduled_arr = scheduled_dep + flight_time
        actual_arr    = actual_dep + flight_time

        rows.append((
            callsign,
            airline,
            dep_airport,
            arr_airport,
            scheduled_dep,
            actual_dep,
            scheduled_arr,
            actual_arr,
            status,
            delay
        ))

    # Vide les anciennes données simulées avant d'insérer
    cur.execute("DELETE FROM raw_flight_status WHERE flight_number IN %s",
                (tuple(r[0] for r in rows),))

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
    print(f"[Simulator] {len(rows)} vols simulés insérés en base")
    return len(rows)

if __name__ == "__main__":
    simulate_delays()