from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os
import sys

sys.path.insert(0, "/opt/airflow/etl")

default_args = {
    "owner":            "airflow",
    "retries":          3,
    "retry_delay":      timedelta(minutes=2),
    "email_on_failure": False,
}

def run_extract():
    import extract
    states = extract.fetch_flights()
    rows   = extract.transform(states)
    extract.load(rows)
    return f"{len(rows)} vols chargés"

def run_aviationstack():
    import extract
    rows = extract.fetch_aviationstack()
    extract.load_aviationstack(rows)
    return f"{len(rows)} statuts chargés"

def run_simulate_delays():
    import simulate_delays
    n = simulate_delays.simulate_delays()
    return f"{n} vols simulés"

def run_analytics():
    import psycopg2
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST", "postgres"),
        port=5432,
        dbname=os.getenv("DB_NAME", "flight_db"),
        user=os.getenv("DB_USER", "flight"),
        password=os.getenv("DB_PASSWORD", "flight"),
    )
    cur = conn.cursor()
    cur.execute("DELETE FROM analytics_zone_congestion;")
    cur.execute("""
        INSERT INTO analytics_zone_congestion (zone_name, flight_count, avg_altitude)
        SELECT
            CASE
                WHEN longitude BETWEEN -10 AND 5  AND latitude BETWEEN 40 AND 55 THEN 'Europe Ouest'
                WHEN longitude BETWEEN  5  AND 20 AND latitude BETWEEN 45 AND 60 THEN 'Europe Centre'
                WHEN longitude BETWEEN 20  AND 35 AND latitude BETWEEN 35 AND 50 THEN 'Europe Est'
                ELSE 'Autre'
            END AS zone_name,
            COUNT(*)      AS flight_count,
            AVG(altitude) AS avg_altitude
        FROM raw_flights
        WHERE ingested_at > NOW() - INTERVAL '1 hour'
        GROUP BY zone_name;
    """)
    conn.commit()
    cur.close()
    conn.close()
    print("Analytics mis à jour")

with DAG(
    dag_id="flight_pipeline",
    default_args=default_args,
    description="Pipeline trafic aérien européen",
    schedule_interval="*/5 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["flight", "etl"],
) as dag:

    t1 = PythonOperator(
        task_id="extract_and_load",
        python_callable=run_extract,
    )

    t2 = PythonOperator(
        task_id="simulate_delays",
        python_callable=run_simulate_delays,
    )

    t3 = PythonOperator(
        task_id="aviationstack",
        python_callable=run_aviationstack,
    )

    t4 = PythonOperator(
        task_id="run_analytics",
        python_callable=run_analytics,
    )

    t5 = BashOperator(
        task_id="health_check",
        bash_command="echo 'Pipeline OK à $(date)'",
    )

    t1 >> [t2, t3] >> t4 >> t5