import plotly.express as px
import streamlit as st
import pandas as pd
from deltalake import DeltaTable

# Load the latest Delta Lake state
dt = DeltaTable("./storage/delta/race_leaderboard")
df = dt.to_pandas()

# Categorize runners into course segments
bins = [0, 2.5, 5.0, 7.5, 9.0, 10.0]
labels = ["Start - 2.5k", "2.5k - 5k", "5k - 7.5k", "7.5k - 9k", "Finished (10k)"]

df["segment"] = pd.cut(df["current_distance_km"], bins=bins, labels=labels, include_lowest=True)
segment_counts = df["segment"].value_counts().reindex(labels).reset_index()
segment_counts.columns = ["Course Segment", "Runner Count"]

# Create shaded bar chart showing runner density
fig = px.bar(
    segment_counts,
    x="Course Segment",
    y="Runner Count",
    color="Runner Count",
    color_continuous_scale="Viridis",
    title="🏃 Live Runner Density Along the Course"
)

st.plotly_chart(fig, use_container_width=True)