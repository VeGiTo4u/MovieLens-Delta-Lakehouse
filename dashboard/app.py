"""
MovieLens Analytics Dashboard — Overview
=========================================
Main entry page showing high-level KPI cards and rating distribution.
"""

import json
import os
import streamlit as st
import plotly.graph_objects as go
from services.data_loader import load_yearly_summary, load_rating_distribution, load_all_time_summary
from config.theme import (
    inject_theme, section_header, sidebar_badges, kpi_card, callout,
    PLOTLY_LAYOUT, CHART_GRADIENT, COLORS,
)

# ── Page Config ──────────────────────────────────────────────
st.set_page_config(
    page_title="MovieLens Analytics",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Theme ────────────────────────────────────────────────────
inject_theme()

# ── Sidebar ──────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## MovieLens Analytics")
    st.markdown("---")
    st.markdown(
        "A **Delta Lakehouse** pipeline powering analytics "
        "across the MovieLens dataset."
    )
    st.markdown("---")
    sidebar_badges(["Delta Lake", "Spark", "DuckDB", "Plotly", "Streamlit"])
    st.markdown("")
    st.caption("Data: Gold Layer / Parquet / DuckDB")

# ── Hero Header ──────────────────────────────────────────────
st.markdown("""
<div class="hero-banner" role="banner">
    <h1>MovieLens Analytics Dashboard</h1>
    <div class="hero-tagline">
        Real-time insights from the MovieLens Delta Lakehouse —
        powered by <strong>DuckDB</strong> and <strong>Plotly</strong>.
    </div>
</div>
""", unsafe_allow_html=True)

# ── Load Data ────────────────────────────────────────────────
df_yearly = load_yearly_summary()
df_rating_dist = load_rating_distribution()
df_all_time = load_all_time_summary()

# ── KPI Cards (All-Time Overview) ───────────────────────────
all_time_ratings = df_all_time["total_ratings"].iloc[0]
all_time_users   = df_all_time["unique_users"].iloc[0]
all_time_movies  = df_all_time["unique_movies"].iloc[0]
weighted_avg_rating = df_all_time["avg_rating"].iloc[0]

# Get latest year for the Delta (growth) indicators
latest = df_yearly.sort_values("year", ascending=False).iloc[0]

col1, col2, col3, col4 = st.columns(4)

with col1:
    kpi_card(
        icon_name="ratings",
        label="Total Ratings (All-Time)",
        value=f"{all_time_ratings:,.0f}",
        delta=f"+{latest['total_ratings']:,.0f} this year",
        delta_color="success",
    )

with col2:
    kpi_card(
        icon_name="users",
        label="Unique Users (All-Time)",
        value=f"{all_time_users:,.0f}",
        delta=f"+{latest['unique_users']:,.0f} this year",
        delta_color="success",
    )

with col3:
    kpi_card(
        icon_name="movies",
        label="Unique Movies (All-Time)",
        value=f"{all_time_movies:,.0f}",
        delta=f"+{latest['unique_movies']:,.0f} this year",
        delta_color="success",
    )

with col4:
    avg_diff = latest['avg_rating'] - weighted_avg_rating
    kpi_card(
        icon_name="star",
        label="Avg Rating (All-Time)",
        value=f"{weighted_avg_rating:.2f}",
        delta=f"{avg_diff:+.2f} latest year",
        delta_color="success" if avg_diff >= 0 else "danger",
    )

st.divider()

# ── Rating Distribution ─────────────────────────────────────
st.markdown(section_header("Rating Value Distribution"), unsafe_allow_html=True)

# Build the rating distribution chart with go.Figure for finer control
fig_dist = go.Figure()

fig_dist.add_trace(go.Bar(
    x=df_rating_dist["rating"],
    y=df_rating_dist["rating_count"],
    text=df_rating_dist["pct_of_total"].apply(lambda v: f"{v:.1f}%"),
    textposition="outside",
    textfont=dict(family="Fira Code, monospace", size=12, color="#F1F5F9"),
    marker=dict(
        color=df_rating_dist["rating_count"],
        colorscale=CHART_GRADIENT,
        line_width=0,
        cornerradius=6,
    ),
    hovertemplate="<b>Rating %{x}</b><br>Count: %{y:,.0f}<br>%{text}<extra></extra>",
    name="Ratings",
))

# Add average rating reference line
avg_rating_val = weighted_avg_rating
fig_dist.add_hline(
    y=0, line_width=0,  # invisible — we use a shape instead
)
fig_dist.add_shape(
    type="line",
    x0=avg_rating_val, x1=avg_rating_val,
    y0=0, y1=1, yref="paper",
    line=dict(color=COLORS["highlight"], width=2, dash="dot"),
)
fig_dist.add_annotation(
    x=avg_rating_val, y=1.05, yref="paper",
    text=f"Avg: {avg_rating_val:.2f}",
    showarrow=False,
    font=dict(color=COLORS["highlight"], size=12, family="Fira Code, monospace"),
)

fig_dist.update_layout(
    **PLOTLY_LAYOUT,
    coloraxis_showscale=False,
    showlegend=False,
    xaxis=dict(dtick=0.5, title="Rating"),
    yaxis=dict(title="Count"),
    height=440,
)
st.plotly_chart(fig_dist, use_container_width=True)

st.divider()

# ── Yearly Summary Table ─────────────────────────────────────
st.markdown(section_header("Year-over-Year Summary"), unsafe_allow_html=True)

df_display = df_yearly.sort_values("year", ascending=False).copy()
df_display.columns = [
    "Year", "Total Ratings", "Unique Users", "Unique Movies",
    "Avg Rating", "Late Arrivals", "Late Arrival %"
]

max_ratings = df_display["Total Ratings"].max()

st.dataframe(
    df_display,
    use_container_width=True,
    hide_index=True,
    column_config={
        "Year": st.column_config.NumberColumn("Year", format="%d"),
        "Total Ratings": st.column_config.ProgressColumn(
            "Total Ratings",
            min_value=0,
            max_value=int(max_ratings),
            format="%d",
        ),
        "Unique Users": st.column_config.NumberColumn("Unique Users", format="%d"),
        "Unique Movies": st.column_config.NumberColumn("Unique Movies", format="%d"),
        "Avg Rating": st.column_config.NumberColumn("Avg Rating", format="%.2f"),
        "Late Arrivals": st.column_config.NumberColumn("Late Arrivals", format="%d"),
        "Late Arrival %": st.column_config.NumberColumn("Late Arrival %", format="%.3f%%"),
    },
)

st.divider()

# ── Pipeline Health Callout ──────────────────────────────────
manifest_path = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "data", ".sync_manifest.json"
)
if os.path.exists(manifest_path):
    try:
        with open(manifest_path) as f:
            manifest = json.load(f)
        last_sync = manifest.get("last_sync", "unknown")
        callout(
            f"Pipeline last synced: <strong>{last_sync}</strong> — "
            f"<span class='status-pill'>Live</span>",
            kind="success",
        )
    except Exception:
        callout("Pipeline sync status unavailable.", kind="warning")
else:
    callout(
        "No sync manifest found. Run <code>python sync_data.py</code> to populate.",
        kind="info",
    )
