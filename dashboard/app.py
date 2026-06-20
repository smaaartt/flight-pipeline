import os
import time
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
import psycopg2

st.set_page_config(
    page_title="✈️ European Air Traffic Dashboard",
    page_icon="✈️",
    layout="wide"
)

DB_CONFIG = {
    "host":     os.getenv("DB_HOST", "localhost"),
    "port":     int(os.getenv("DB_PORT", 5432)),
    "dbname":   os.getenv("DB_NAME", "flight_db"),
    "user":     os.getenv("DB_USER", "flight"),
    "password": os.getenv("DB_PASSWORD", "flight"),
}

@st.cache_resource
def get_connection():
    return psycopg2.connect(**DB_CONFIG)

def query(sql):
    try:
        conn = get_connection()
        return pd.read_sql(sql, conn)
    except Exception as e:
        st.error(f"Erreur DB : {e}")
        return pd.DataFrame()

st.title("✈️ European Air Traffic — Performance & Delays Dashboard")
st.caption("ETL/ELT Pipeline + Kafka Streaming + Airflow Orchestration | ESILV MSc A4")

refresh = st.sidebar.slider("Refresh (secondes)", 10, 120, 30)
st.sidebar.markdown("---")
st.sidebar.markdown("**Monitored Airports**")
st.sidebar.markdown("CDG • LHR • FRA • AMS\nMAD • FCO • BCN • MUC")

col1, col2, col3, col4, col5 = st.columns(5)

total     = query("SELECT COUNT(*) AS n FROM raw_flights")
airborne  = query("SELECT COUNT(*) AS n FROM raw_flights WHERE on_ground = false")
countries = query("SELECT COUNT(DISTINCT origin_country) AS n FROM raw_flights")
streaming = query("SELECT COUNT(*) AS n FROM streaming_flights")
delayed   = query("SELECT COUNT(*) AS n FROM raw_flight_status WHERE delay_minutes > 15")

col1.metric("Total Flights Ingested",   int(total["n"].iloc[0])     if not total.empty     else 0)
col2.metric("Currently in flight",  int(airborne["n"].iloc[0])  if not airborne.empty  else 0)
col3.metric("Countries of Origin",       int(countries["n"].iloc[0]) if not countries.empty else 0)
col4.metric("Messages Kafka received", int(streaming["n"].iloc[0]) if not streaming.empty else 0)
col5.metric("Flights Delayed",       int(delayed["n"].iloc[0])   if not delayed.empty   else 0)

st.divider()

tab1, tab2, tab3 = st.tabs(["🗺️ Live Traffic", "⏱️ Delay Analysis", "🌍 Country Analysis"])
with tab1:

    st.subheader("🗺️ Real-Time Flight Positions")
    df_map = query("""
        SELECT icao24, callsign, origin_country,
               longitude, latitude, altitude, velocity
        FROM raw_flights
        WHERE longitude IS NOT NULL AND latitude IS NOT NULL
        AND ingested_at > NOW() - INTERVAL '24 hours'
        LIMIT 3000
    """)
    if not df_map.empty:
        fig1 = px.scatter_geo(
            df_map, lat="latitude", lon="longitude",
            hover_name="callsign",
            hover_data={"origin_country": True, "altitude": True, "velocity": True},
            color="altitude",
            color_continuous_scale="Viridis",
            scope="europe",
            title="Active Flights Over Europe"
        )
        fig1.update_traces(marker=dict(size=4))
        st.plotly_chart(fig1, use_container_width=True)
    else:
        st.info("En attente des données OpenSky...")

    st.subheader("🛬 Airport Saturation (Planes on Ground)")
    df_ground = query("""
        SELECT airport, COUNT(*) AS planes_on_ground
        FROM (
            SELECT
                CASE
                    WHEN longitude BETWEEN 2.3 AND 2.7   AND latitude BETWEEN 48.9 AND 49.1 THEN 'CDG'
                    WHEN longitude BETWEEN -0.5 AND 0.1  AND latitude BETWEEN 51.4 AND 51.6 THEN 'LHR'
                    WHEN longitude BETWEEN 8.5 AND 8.7   AND latitude BETWEEN 50.0 AND 50.1 THEN 'FRA'
                    WHEN longitude BETWEEN 4.7 AND 4.9   AND latitude BETWEEN 52.2 AND 52.4 THEN 'AMS'
                    WHEN longitude BETWEEN -3.7 AND -3.5 AND latitude BETWEEN 40.4 AND 40.5 THEN 'MAD'
                    WHEN longitude BETWEEN 12.2 AND 12.4 AND latitude BETWEEN 41.7 AND 41.9 THEN 'FCO'
                    WHEN longitude BETWEEN 2.0 AND 2.2   AND latitude BETWEEN 41.2 AND 41.4 THEN 'BCN'
                    WHEN longitude BETWEEN 11.7 AND 11.9 AND latitude BETWEEN 48.3 AND 48.4 THEN 'MUC'
                END AS airport
            FROM raw_flights
            WHERE on_ground = true
            AND ingested_at > NOW() - INTERVAL '24 hours'
        ) sub
        WHERE airport IS NOT NULL
        GROUP BY airport
        ORDER BY planes_on_ground DESC
    """)
    if not df_ground.empty:
        fig2 = px.bar(
            df_ground, x="airport", y="planes_on_ground",
            color="planes_on_ground",
            color_continuous_scale="Reds",
            labels={"airport": "AAirport", "planes_on_ground": "Planes on Ground"},
            title="Number of Planes on Ground by Airport (Last 24 Hours)"
        )
        st.plotly_chart(fig2, use_container_width=True)
    else:
        st.info("En attente des données de saturation...")

    st.subheader("📈 French Traffic Evolution (Last 24h)")
    df_hour = query("""
        SELECT DATE_TRUNC('hour', ingested_at) AS hour,
               COUNT(*) AS flight_count
        FROM raw_flights
        WHERE ingested_at > NOW() - INTERVAL '24 hours'
          AND origin_country = 'France'
        GROUP BY DATE_TRUNC('hour', ingested_at)
        ORDER BY hour
    """)
    if not df_hour.empty:
        fig3 = px.line(
            df_hour, x="hour", y="flight_count",
            markers=True,
            labels={"hour": "Heure", "flight_count": "Nombre de vols"},
            color_discrete_sequence=["#1f77b4"]
        )
        st.plotly_chart(fig3, use_container_width=True)
    else:
        st.info("En attente des données historiques...")

with tab2:

    st.subheader("⏱️ Average Delay by Departure Airport")
    df_delay_airport = query("""
        SELECT departure_airport, total_flights, avg_delay,
               delayed_flights, cancelled_flights
        FROM v_delays_by_airport
        LIMIT 10
    """)
    if not df_delay_airport.empty:
        fig4 = px.bar(
            df_delay_airport,
            x="departure_airport", y="avg_delay",
            color="avg_delay",
            color_continuous_scale="Oranges",
            labels={"departure_airport": "Aéroport", "avg_delay": "Retard moyen (min)"},
            title="Retard moyen par aéroport de départ"
        )
        st.plotly_chart(fig4, use_container_width=True)
        st.dataframe(df_delay_airport.rename(columns={
            "departure_airport": "Aéroport",
            "total_flights":     "Total vols",
            "avg_delay":         "Retard moyen (min)",
            "delayed_flights":   "Vols en retard",
            "cancelled_flights": "Annulés"
        }), use_container_width=True)
    else:
        st.info("Les données de retard seront disponibles après le premier appel AviationStack.")

    st.subheader("🏢 Top Airlines by Average Delay")
    df_delay_airline = query("""
        SELECT airline, total_flights, avg_delay, delayed_flights
        FROM v_delays_by_airline
        LIMIT 10
    """)
    if not df_delay_airline.empty:
        fig5 = px.bar(
            df_delay_airline,
            x="avg_delay", y="airline",
            orientation="h",
            color="avg_delay",
            color_continuous_scale="Reds",
            labels={"airline": "Compagnie", "avg_delay": "Retard moyen (min)"}
        )
        fig5.update_layout(yaxis=dict(autorange="reversed"))
        st.plotly_chart(fig5, use_container_width=True)
    else:
        st.info("Les données de retard seront disponibles après le premier appel AviationStack.")

    st.subheader("📊 Flight Status Distribution")
    df_status = query("""
        SELECT status, COUNT(*) AS count
        FROM raw_flight_status
        WHERE status IS NOT NULL
        GROUP BY status
        ORDER BY count DESC
    """)
    if not df_status.empty:
        fig6 = px.pie(
            df_status, names="status", values="count",
            color_discrete_sequence=px.colors.qualitative.Set3,
            title="Répartition des statuts de vols"
        )
        st.plotly_chart(fig6, use_container_width=True)
    else:
        st.info("En attente des données AviationStack...")

with tab3:

    st.subheader("🌍 Top 15 Countries by Traffic Volume")
    df_country = query("""
        SELECT origin_country, COUNT(*) AS total_flights
        FROM raw_flights
        WHERE origin_country IS NOT NULL
        GROUP BY origin_country
        ORDER BY total_flights DESC
        LIMIT 15
    """)
    if not df_country.empty:
        fig7 = px.bar(
            df_country,
            x="total_flights", y="origin_country",
            orientation="h",
            color="total_flights",
            color_continuous_scale="Blues",
            labels={"total_flights": "Nombre de vols", "origin_country": "Pays"}
        )
        fig7.update_layout(yaxis=dict(autorange="reversed"))
        st.plotly_chart(fig7, use_container_width=True)
    else:
        st.info("En attente des données...")

    st.subheader("⚖️ Vols au sol vs en vol (streaming Kafka)")
    df_ground_air = query("SELECT * FROM v_ground_vs_air")
    if not df_ground_air.empty:
        fig8 = px.pie(
            df_ground_air, names="status", values="count",
            color_discrete_sequence=["#2196F3", "#FF9800"],
            title="Répartition au sol / en vol"
        )
        st.plotly_chart(fig8, use_container_width=True)
    else:
        st.info("En attente des données...")

time.sleep(refresh)
st.rerun()