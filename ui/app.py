import streamlit as st
import pandas as pd
from deltalake import DeltaTable
import time

st.set_page_config(page_title="10k Race Live Tracker", page_icon="🏃", layout="wide")

st.title("🏃 Live 10k Race Leaderboard")
st.caption("Real-Time Analytics powered by PySpark Structured Streaming, Apache Kafka & Delta Lake")

# Dashboard placeholder for live refreshes
placeholder = st.empty()

def load_delta_data():
    """Reads the latest Delta Lake state natively without starting a PySpark JVM."""
    dt = DeltaTable("./storage/delta/race_leaderboard")
    return dt.to_pandas()

while True:
    try:
        df = load_delta_data()
        
        if not df.empty:
            # Sort by distance (descending) and average pace (ascending)
            df = df.sort_values(by=["current_distance_km", "avg_pace_min_per_km"], ascending=[False, True])
            
            with placeholder.container():
                # Top KPI Metrics
                col1, col2, col3 = st.columns(3)
                col1.metric("Active Runners Tracked", len(df))
                
                finishers = len(df[df["current_distance_km"] == 10.0])
                col2.metric("Finishers (10k)", finishers)
                
                fastest_pace = df["avg_pace_min_per_km"].min()
                col3.metric("Fastest Pace", f"{fastest_pace} min/km")

                st.divider()

                # Leaderboard Table
                st.subheader("📊 Top 15 Leaders")
                display_cols = ["bib_number", "runner_name", "current_distance_km", "elapsed_minutes", "avg_pace_min_per_km"]
                st.dataframe(df[display_cols].head(15), use_container_width=True)

    except Exception as e:
        with placeholder.container():
            st.warning("⏳ Waiting for Delta Lake table initialization from PySpark...")

    time.sleep(2)  # Refresh UI every 2 seconds