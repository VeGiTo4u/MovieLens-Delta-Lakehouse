"""
Page 2 — Movies
=================
Top 30 highest rated movies and top 30 most popular movies.
"""

import streamlit as st
import plotly.express as px
from data_loader import load_top_rated_movies, load_most_popular_movies
from theme import inject_theme, section_header, PLOTLY_LAYOUT, CHART_GRADIENT

st.set_page_config(page_title="Movies | MovieLens", page_icon="M", layout="wide")
inject_theme()

st.markdown("# Movie Rankings")
st.markdown("Discover the highest rated and most popular movies on the platform.")
st.markdown("")

# ── Load Data ────────────────────────────────────────────────
df_top_rated = load_top_rated_movies()
df_most_popular = load_most_popular_movies()

# ── Top Rated ────────────────────────────────────────────────
st.markdown(
    section_header("Top 30 Highest Rated Movies", "min. 100 ratings"),
    unsafe_allow_html=True,
)

df_top_rated_sorted = df_top_rated.sort_values("avg_rating", ascending=True).tail(30)

fig_rated = px.bar(
    df_top_rated_sorted,
    x="avg_rating",
    y="title",
    orientation="h",
    color="avg_rating",
    color_continuous_scale=CHART_GRADIENT,
    custom_data=["rating_count", "release_year"],
    labels={"avg_rating": "Average Rating", "title": ""},
)
fig_rated.update_traces(
    hovertemplate="<b>%{y}</b><br>Avg Rating: %{x:.2f}<br>Ratings: %{customdata[0]:,}<br>Year: %{customdata[1]}<extra></extra>",
    marker_line_width=0,
    marker_cornerradius=4,
)
fig_rated.update_layout(
    **{**PLOTLY_LAYOUT, "margin": dict(t=20, b=30, l=250, r=20)},
    coloraxis_showscale=False,
    height=max(500, len(df_top_rated_sorted) * 22),
    xaxis=dict(range=[3.0, 5.0]),
)
st.plotly_chart(fig_rated, use_container_width=True)

# ── Most Popular ─────────────────────────────────────────────
st.markdown(
    section_header("Top 30 Most Popular Movies", "by rating count"),
    unsafe_allow_html=True,
)

df_popular_sorted = df_most_popular.sort_values("rating_count", ascending=True).tail(30)

fig_popular = px.bar(
    df_popular_sorted,
    x="rating_count",
    y="title",
    orientation="h",
    color="avg_rating",
    color_continuous_scale=CHART_GRADIENT,
    custom_data=["avg_rating", "release_year"],
    labels={"rating_count": "Total Ratings", "title": "", "avg_rating": "Avg Rating"},
)
fig_popular.update_traces(
    hovertemplate="<b>%{y}</b><br>Ratings: %{x:,}<br>Avg Rating: %{customdata[0]:.2f}<br>Year: %{customdata[1]}<extra></extra>",
    marker_line_width=0,
    marker_cornerradius=4,
)
fig_popular.update_layout(
    **{**PLOTLY_LAYOUT, "margin": dict(t=20, b=30, l=250, r=20)},
    height=max(500, len(df_popular_sorted) * 22),
    coloraxis_colorbar=dict(
        title="Avg Rating",
        title_font=dict(size=11, family="Fira Code, monospace"),
        tick_font=dict(size=10),
    ),
)
st.plotly_chart(fig_popular, use_container_width=True)
