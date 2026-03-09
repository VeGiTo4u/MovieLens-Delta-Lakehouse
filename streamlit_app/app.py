"""
MovieLens Analytics Dashboard — Overview
=========================================
Main entry page showing high-level KPI cards and rating distribution.
"""

import streamlit as st
import plotly.express as px
from data_loader import load_yearly_summary, load_rating_distribution, load_all_time_summary

# ── Page Config ──────────────────────────────────────────────
st.set_page_config(
    page_title="MovieLens Analytics",
    page_icon="M",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Custom CSS ───────────────────────────────────────────────
st.markdown("""
<style>
    /* Glassmorphism metric cards */
    div[data-testid="stMetric"] {
        background: linear-gradient(135deg, rgba(108,99,255,0.15), rgba(108,99,255,0.05));
        border: 1px solid rgba(108,99,255,0.25);
        border-radius: 12px;
        padding: 16px 20px;
        backdrop-filter: blur(10px);
        transition: transform 0.2s ease, box-shadow 0.2s ease;
    }
    div[data-testid="stMetric"]:hover {
        transform: translateY(-2px);
        box-shadow: 0 8px 25px rgba(108,99,255,0.2);
    }
    div[data-testid="stMetric"] label {
        color: #A0A0B0 !important;
        font-size: 0.85rem !important;
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }
    div[data-testid="stMetric"] [data-testid="stMetricValue"] {
        color: #FAFAFA !important;
        font-weight: 700 !important;
    }

    /* Sidebar styling */
    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #0E1117 0%, #1A1F2E 100%);
    }

    /* Section dividers */
    .section-header {
        font-size: 1.4rem;
        font-weight: 600;
        color: #FAFAFA;
        margin: 1.5rem 0 0.5rem 0;
        padding-bottom: 0.3rem;
        border-bottom: 2px solid rgba(108,99,255,0.4);
    }

    /* Hide Streamlit branding */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
</style>
""", unsafe_allow_html=True)

# ── Sidebar ──────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## MovieLens Analytics")
    st.markdown("---")
    st.markdown(
        "A **Delta Lakehouse** pipeline powering analytics "
        "across the MovieLens dataset."
    )
    st.markdown("---")
    st.caption("Data: Gold Layer → Parquet → DuckDB")
    st.caption("Built with Streamlit + Plotly")

# ── Title ────────────────────────────────────────────────────
st.markdown("# MovieLens Analytics Dashboard")
st.markdown(
    "Real-time insights from the MovieLens Delta Lakehouse — "
    "powered by **DuckDB** and **Plotly**."
)
st.markdown("")

# ── Load Data ────────────────────────────────────────────────
df_yearly = load_yearly_summary()
df_rating_dist = load_rating_distribution()
df_all_time = load_all_time_summary()

# ── KPI Cards (All-Time Overview) ───────────────────────────
# Extracted true all-time unique values directly from Gold layer
all_time_ratings = df_all_time["total_ratings"].iloc[0]
all_time_users   = df_all_time["unique_users"].iloc[0]
all_time_movies  = df_all_time["unique_movies"].iloc[0]
weighted_avg_rating = df_all_time["avg_rating"].iloc[0]

# Get latest year for the Delta (growth) indicators
latest = df_yearly.sort_values("year", ascending=False).iloc[0]

col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric(
        label="Total Ratings (All-Time)",
        value=f"{all_time_ratings:,.0f}",
        delta=f"+{latest['total_ratings']:,.0f} this year",
    )

with col2:
    st.metric(
        label="Unique Users (All-Time)",
        value=f"{all_time_users:,.0f}",
        delta=f"+{latest['unique_users']:,.0f} this year",
    )

with col3:
    st.metric(
        label="Unique Movies (All-Time)",
        value=f"{all_time_movies:,.0f}",
        delta=f"+{latest['unique_movies']:,.0f} this year",
    )

with col4:
    # Delta for average rating is the absolute difference between all-time and latest year
    avg_diff = latest['avg_rating'] - weighted_avg_rating
    st.metric(
        label="Avg Rating (All-Time)",
        value=f"{weighted_avg_rating:.2f}",
        delta=f"{avg_diff:+.2f} latest year",
    )

st.markdown("")

# ── Rating Distribution ─────────────────────────────────────
st.markdown('<p class="section-header">Rating Value Distribution</p>', unsafe_allow_html=True)

fig_dist = px.bar(
    df_rating_dist,
    x="rating",
    y="rating_count",
    text="pct_of_total",
    color="rating_count",
    color_continuous_scale=["#2D2B55", "#6C63FF", "#A78BFA"],
    labels={"rating": "Rating", "rating_count": "Count", "pct_of_total": "% of Total"},
)
fig_dist.update_traces(
    texttemplate="%{text:.1f}%",
    textposition="outside",
    marker_line_width=0,
)
fig_dist.update_layout(
    template="plotly_dark",
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    coloraxis_showscale=False,
    xaxis=dict(dtick=0.5),
    height=420,
    margin=dict(t=30, b=40),
    font=dict(family="Inter, sans-serif"),
)
st.plotly_chart(fig_dist, use_container_width=True)

# ── Yearly Summary Table ─────────────────────────────────────
st.markdown('<p class="section-header">Year-over-Year Summary</p>', unsafe_allow_html=True)

df_display = df_yearly.sort_values("year", ascending=False).copy()
df_display.columns = [
    "Year", "Total Ratings", "Unique Users", "Unique Movies",
    "Avg Rating", "Late Arrivals", "Late Arrival %"
]

st.dataframe(
    df_display.style.format({
        "Total Ratings": "{:,.0f}",
        "Unique Users": "{:,.0f}",
        "Unique Movies": "{:,.0f}",
        "Avg Rating": "{:.2f}",
        "Late Arrivals": "{:,.0f}",
        "Late Arrival %": "{:.3f}%",
    }).background_gradient(
        subset=["Total Ratings"],
        cmap="Purples",
    ),
    use_container_width=True,
    hide_index=True,
)
