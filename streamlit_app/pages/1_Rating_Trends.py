"""
Page 1 — Rating Trends
=======================
Monthly rating trends (line + bar) and yearly summary table.
"""

import streamlit as st
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from data_loader import load_rating_trends_monthly, load_yearly_summary

st.set_page_config(page_title="Rating Trends | MovieLens", page_icon="M", layout="wide")

# ── Custom CSS ───────────────────────────────────────────────
st.markdown("""
<style>
    .section-header {
        font-size: 1.4rem; font-weight: 600; color: #FAFAFA;
        margin: 1.5rem 0 0.5rem 0; padding-bottom: 0.3rem;
        border-bottom: 2px solid rgba(108,99,255,0.4);
    }
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
</style>
""", unsafe_allow_html=True)

st.markdown("# Rating Trends")
st.markdown("How have ratings evolved month-over-month and year-over-year?")
st.markdown("")

# ── Load Data ────────────────────────────────────────────────
df_monthly = load_rating_trends_monthly()
df_yearly = load_yearly_summary()

# ── Monthly Trends — Dual Axis ───────────────────────────────
st.markdown('<p class="section-header">Monthly Rating Trends</p>', unsafe_allow_html=True)

df_monthly["date_label"] = (
    df_monthly["year"].astype(str) + "-" + df_monthly["month"].astype(str).str.zfill(2)
)

fig = make_subplots(specs=[[{"secondary_y": True}]])

# Bar: rating volume
fig.add_trace(
    go.Bar(
        x=df_monthly["date_label"],
        y=df_monthly["rating_count"],
        name="Rating Count",
        marker_color="rgba(108,99,255,0.35)",
        marker_line_width=0,
    ),
    secondary_y=False,
)

# Line: average rating
fig.add_trace(
    go.Scatter(
        x=df_monthly["date_label"],
        y=df_monthly["avg_rating"],
        name="Avg Rating",
        line=dict(color="#A78BFA", width=2.5),
        mode="lines",
    ),
    secondary_y=True,
)

fig.update_layout(
    template="plotly_dark",
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    height=450,
    margin=dict(t=30, b=60),
    legend=dict(orientation="h", y=1.08, x=0.5, xanchor="center"),
    font=dict(family="Inter, sans-serif"),
    hovermode="x unified",
)
fig.update_yaxes(title_text="Rating Count", secondary_y=False)
fig.update_yaxes(title_text="Avg Rating", secondary_y=True, range=[2.5, 5.0])

st.plotly_chart(fig, use_container_width=True)

# ── Monthly Users & Movies ───────────────────────────────────
st.markdown('<p class="section-header">Monthly Unique Users & Movies</p>', unsafe_allow_html=True)

col1, col2 = st.columns(2)

with col1:
    fig_users = go.Figure()
    fig_users.add_trace(go.Scatter(
        x=df_monthly["date_label"],
        y=df_monthly["unique_users"],
        fill="tozeroy",
        fillcolor="rgba(108,99,255,0.15)",
        line=dict(color="#6C63FF", width=2),
        name="Unique Users",
    ))
    fig_users.update_layout(
        template="plotly_dark",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        height=300,
        margin=dict(t=30, b=50),
        title=dict(text="Unique Users per Month", font=dict(size=14)),
        font=dict(family="Inter, sans-serif"),
    )
    st.plotly_chart(fig_users, use_container_width=True)

with col2:
    fig_movies = go.Figure()
    fig_movies.add_trace(go.Scatter(
        x=df_monthly["date_label"],
        y=df_monthly["unique_movies"],
        fill="tozeroy",
        fillcolor="rgba(167,139,250,0.15)",
        line=dict(color="#A78BFA", width=2),
        name="Unique Movies",
    ))
    fig_movies.update_layout(
        template="plotly_dark",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        height=300,
        margin=dict(t=30, b=50),
        title=dict(text="Unique Movies Rated per Month", font=dict(size=14)),
        font=dict(family="Inter, sans-serif"),
    )
    st.plotly_chart(fig_movies, use_container_width=True)

# ── Yearly Summary Table ─────────────────────────────────────
st.markdown('<p class="section-header">Yearly Summary</p>', unsafe_allow_html=True)

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
    }),
    use_container_width=True,
    hide_index=True,
)
