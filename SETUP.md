# ⚙️ Setup Guide — Flight Pipeline

## Prerequisites

- Docker Desktop installed and running
- Git
- OpenSky Network account → https://opensky-network.org
- AviationStack account → https://aviationstack.com/signup/free

---

## 1. Clone the repository

```bash
git clone https://github.com/[your-username]/flight-pipeline.git
cd flight-pipeline
```

## 2. Create the `.env` file

Create a `.env` file at the root of the project:

```env
# OpenSky
OPENSKY_USER=your_opensky_username
OPENSKY_PASS=your_opensky_password

# AviationStack
AVIATIONSTACK_KEY=your_aviationstack_api_key

# PostgreSQL
DB_HOST=postgres
DB_PORT=5432
DB_NAME=flight_db
DB_USER=flight
DB_PASSWORD=flight

# Kafka
KAFKA_BOOTSTRAP=kafka:29092
```

## 3. Start all services

```bash
docker-compose up --build -d
```

Wait 2-3 minutes for all services to be healthy.

## 4. Initialize Airflow

```bash
docker-compose run airflow-init
```

## 5. Load initial data

```bash
# Load first batch of flights
docker-compose exec etl python extract.py

# Generate simulated delays
docker-compose exec etl python simulate_delays.py
```

## 6. Activate the Airflow DAG

Go to http://localhost:8081 (admin / admin) and activate the `flight_pipeline` DAG.

## 7. Access the dashboard

Open http://localhost:8501

---

## 🔍 Useful commands

```bash
# Check all services status
docker-compose ps

# View ETL logs
docker-compose logs -f etl

# View Kafka producer logs
docker-compose logs -f producer

# View Kafka consumer logs
docker-compose logs -f consumer

# Stop everything
docker-compose down

# Stop and delete all data
docker-compose down -v
```

---

## 📊 Services & Ports

| Service | URL | Credentials |
|---|---|---|
| Streamlit Dashboard | http://localhost:8501 | — |
| Airflow | http://localhost:8081 | admin / admin |
| PostgreSQL | localhost:5432 | flight / flight |
| Kafka | localhost:9092 | — |