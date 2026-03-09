"""
Page 2 — Movies
=================
Top 30 highest rated movies and top 30 most popular movies.
"""

import streamlit as st
import plotly.express as px
from data_loader import load_top_rated_movies, load_most_popular_movies

st.set_page_config(page_title="Movies | MovieLens", page_icon="🎬", layout="wide")

st.markdown("""
<style>
    .section-header {
        font-size: 1.4rem; font-weight: 600; color: #FAFAFA;
        margin: 1.5rem 0 0.5rem 0; padding-bottom: 0.3rem;
        border-bottom: 2px solid rgba(108,99,255,0.4);
    }
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}
</style>
""", unsafe_allow_html=True)

st.markdown("# 🎬 Movie Rankings")
st.markdown("Discover the highest rated and most popular movies on the platform.")
st.markdown("")

# ── Load Data ────────────────────────────────────────────────
df_top_rated = load_top_rated_movies()
df_most_popular = load_most_popular_movies()

# ── Top Rated ────────────────────────────────────────────────
st.markdown('<p class="section-header">🏆 Top 30 Highest Rated Movies <small style="color:#A0A0B0;">(min. 100 ratings)</small></p>', unsafe_allow_html=True)

df_top_rated_sorted = df_top_rated.sort_values("avg_rating", ascending=True).tail(30)

fig_rated = px.bar(
    df_top_rated_sorted,
    x="avg_rating",
    y="title",
    orientation="h",
    color="avg_rating",
    color_continuous_scale=["#2D2B55", "#6C63FF", "#A78BFA", "#C4B5FD"],
    custom_data=["rating_count", "release_year"],
    labels={"avg_rating": "Average Rating", "title": ""},
)
fig_rated.update_traces(
    hovertemplate="<b>%{y}</b><br>Avg Rating: %{x:.2f}<br>Ratings: %{customdata[0]:,}<br>Year: %{customdata[1]}<extra></extra>",
    marker_line_width=0,
)
fig_rated.update_layout(
    template="plotly_dark",
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    coloraxis_showscale=False,
    height=max(500, len(df_top_rated_sorted) * 22),
    margin=dict(t=20, b=30, l=250),
    font=dict(family="Inter, sans-serif"),
    xaxis=dict(range=[3.0, 5.0]),
)
st.plotly_chart(fig_rated, use_container_width=True)

# ── Most Popular ─────────────────────────────────────────────
st.markdown('<p class="section-header">🔥 Top 30 Most Popular Movies <small style="color:#A0A0B0;">(by rating count)</small></p>', unsafe_allow_html=True)

df_popular_sorted = df_most_popular.sort_values("rating_count", ascending=True).tail(30)

fig_popular = px.bar(
    df_popular_sorted,
    x="rating_count",
    y="title",
    orientation="h",
    color="avg_rating",
    color_continuous_scale=["#374151", "#6C63FF", "#A78BFA"],
    custom_data=["avg_rating", "release_year"],
    labels={"rating_count": "Total Ratings", "title": "", "avg_rating": "Avg Rating"},
)
fig_popular.update_traces(
    hovertemplate="<b>%{y}</b><br>Ratings: %{x:,}<br>Avg Rating: %{customdata[0]:.2f}<br>Year: %{customdata[1]}<extra></extra>",
    marker_line_width=0,
)
fig_popular.update_layout(
    template="plotly_dark",
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    height=max(500, len(df_popular_sorted) * 22),
    margin=dict(t=20, b=30, l=250),
    font=dict(family="Inter, sans-serif"),
    coloraxis_colorbar=dict(title="Avg Rating"),
)
st.plotly_chart(fig_popular, use_container_width=True)
