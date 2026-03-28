"""
Page 1 — Rating Trends
=======================
Monthly rating trends (line + bar) and yearly summary table.
"""

import streamlit as st
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from services.data_loader import load_rating_trends_monthly, load_yearly_summary
from config.theme import inject_theme, section_header, PLOTLY_LAYOUT, COLORS

st.set_page_config(page_title="Rating Trends | MovieLens", page_icon="M", layout="wide")
inject_theme()

st.markdown("# Rating Trends")
st.markdown("How have ratings evolved month-over-month and year-over-year?")
st.markdown("")

# ── Load Data ────────────────────────────────────────────────
df_monthly = load_rating_trends_monthly()
df_yearly = load_yearly_summary()

# ── Monthly Trends — Dual Axis ───────────────────────────────
st.markdown(section_header("Monthly Rating Trends", "volume vs. quality"), unsafe_allow_html=True)

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
        marker_color="rgba(108,99,255,0.30)",
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
        line=dict(color=COLORS["accent"], width=2.5, shape="spline"),
        mode="lines",
        fill="tonexty",
        fillcolor="rgba(167,139,250,0.06)",
    ),
    secondary_y=True,
)

fig.update_layout(
    **PLOTLY_LAYOUT,
    height=450,
    hovermode="x unified",
)
fig.update_yaxes(title_text="Rating Count", secondary_y=False)
fig.update_yaxes(title_text="Avg Rating", secondary_y=True, range=[2.5, 5.0])

st.plotly_chart(fig, use_container_width=True)

# ── Monthly Users & Movies ───────────────────────────────────
st.markdown(section_header("Monthly Unique Users & Movies"), unsafe_allow_html=True)

col1, col2 = st.columns(2)

with col1:
    fig_users = go.Figure()
    fig_users.add_trace(go.Scatter(
        x=df_monthly["date_label"],
        y=df_monthly["unique_users"],
        fill="tozeroy",
        fillcolor="rgba(108,99,255,0.12)",
        line=dict(color=COLORS["primary"], width=2, shape="spline"),
        name="Unique Users",
    ))
    fig_users.update_layout(
        **PLOTLY_LAYOUT,
        height=300,
        title=dict(text="Unique Users per Month", font=dict(size=14, family="Fira Code, monospace")),
    )
    st.plotly_chart(fig_users, use_container_width=True)

with col2:
    fig_movies = go.Figure()
    fig_movies.add_trace(go.Scatter(
        x=df_monthly["date_label"],
        y=df_monthly["unique_movies"],
        fill="tozeroy",
        fillcolor="rgba(167,139,250,0.12)",
        line=dict(color=COLORS["accent"], width=2, shape="spline"),
        name="Unique Movies",
    ))
    fig_movies.update_layout(
        **PLOTLY_LAYOUT,
        height=300,
        title=dict(text="Unique Movies Rated per Month", font=dict(size=14, family="Fira Code, monospace")),
    )
    st.plotly_chart(fig_movies, use_container_width=True)

# ── Yearly Summary Table ─────────────────────────────────────
st.markdown(section_header("Yearly Summary"), unsafe_allow_html=True)

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
