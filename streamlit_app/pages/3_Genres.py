"""
Page 3 — Genres
================
Genre performance breakdown and genre popularity trends over time.
"""

import streamlit as st
import plotly.express as px
from data_loader import load_genre_performance, load_genre_trends_yearly

st.set_page_config(page_title="Genres | MovieLens", page_icon="M", layout="wide")

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

st.markdown("# Genre Analysis")
st.markdown("How do genres compare in popularity and quality? How have they evolved over time?")
st.markdown("")

# ── Load Data ────────────────────────────────────────────────
df_genre = load_genre_performance()
df_genre_trends = load_genre_trends_yearly()

# ── Genre Performance ────────────────────────────────────────
st.markdown('<p class="section-header">Genre Performance Summary</p>', unsafe_allow_html=True)

col1, col2 = st.columns(2)

with col1:
    df_sorted = df_genre.sort_values("rating_count", ascending=True)
    fig_count = px.bar(
        df_sorted,
        x="rating_count",
        y="genre_name",
        orientation="h",
        color="avg_rating",
        color_continuous_scale=["#374151", "#6C63FF", "#A78BFA", "#C4B5FD"],
        custom_data=["avg_rating", "unique_movies", "unique_users"],
        labels={"rating_count": "Total Ratings", "genre_name": "", "avg_rating": "Avg Rating"},
    )
    fig_count.update_traces(
        hovertemplate="<b>%{y}</b><br>Ratings: %{x:,}<br>Avg Rating: %{customdata[0]:.2f}<br>Movies: %{customdata[1]:,}<br>Users: %{customdata[2]:,}<extra></extra>",
        marker_line_width=0,
    )
    fig_count.update_layout(
        template="plotly_dark",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        height=550,
        margin=dict(t=30, b=30, l=120),
        title=dict(text="Ratings by Genre", font=dict(size=14)),
        coloraxis_colorbar=dict(title="Avg Rating"),
        font=dict(family="Inter, sans-serif"),
    )
    st.plotly_chart(fig_count, use_container_width=True)

with col2:
    df_avg_sorted = df_genre.sort_values("avg_rating", ascending=True)
    fig_avg = px.bar(
        df_avg_sorted,
        x="avg_rating",
        y="genre_name",
        orientation="h",
        color="rating_count",
        color_continuous_scale=["#1E1B4B", "#4338CA", "#6C63FF", "#A78BFA"],
        custom_data=["rating_count", "unique_movies"],
        labels={"avg_rating": "Average Rating", "genre_name": "", "rating_count": "Ratings"},
    )
    fig_avg.update_traces(
        hovertemplate="<b>%{y}</b><br>Avg Rating: %{x:.2f}<br>Ratings: %{customdata[0]:,}<br>Movies: %{customdata[1]:,}<extra></extra>",
        marker_line_width=0,
    )
    fig_avg.update_layout(
        template="plotly_dark",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        height=550,
        margin=dict(t=30, b=30, l=120),
        title=dict(text="Average Rating by Genre", font=dict(size=14)),
        coloraxis_colorbar=dict(title="Count"),
        font=dict(family="Inter, sans-serif"),
    )
    st.plotly_chart(fig_avg, use_container_width=True)

# ── Genre Trends Heatmap ─────────────────────────────────────
st.markdown('<p class="section-header">Genre Popularity Over Time</p>', unsafe_allow_html=True)

pivot = df_genre_trends.pivot_table(
    index="genre_name", columns="year", values="rating_count", fill_value=0
)

fig_heat = px.imshow(
    pivot,
    color_continuous_scale=["#0E1117", "#2D2B55", "#6C63FF", "#A78BFA", "#C4B5FD"],
    labels=dict(x="Year", y="Genre", color="Ratings"),
    aspect="auto",
)
fig_heat.update_layout(
    template="plotly_dark",
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    height=500,
    margin=dict(t=30, b=30, l=120),
    font=dict(family="Inter, sans-serif"),
)
st.plotly_chart(fig_heat, use_container_width=True)
