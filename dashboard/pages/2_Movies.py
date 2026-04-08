"""
Page 2 — Movies
=================
Bubble chart overview, plus top-N highest rated and most popular movies
in a tabbed layout for clean drilldown.
"""

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from dashboard.services.data_loader import load_top_rated_movies, load_most_popular_movies
from dashboard.config.theme import inject_theme, section_header, callout, PLOTLY_LAYOUT, CHART_GRADIENT, COLORS

st.set_page_config(page_title="Movies | MovieLens", layout="wide")
inject_theme()

st.markdown("# Movie Rankings")
st.markdown("Discover the highest rated and most popular movies on the platform.")
st.markdown("")

# ── Load Data ────────────────────────────────────────────────
df_top_rated = load_top_rated_movies()
df_most_popular = load_most_popular_movies()

# ── Rating vs. Popularity Bubble Chart ───────────────────────
st.markdown(
    section_header("Rating vs. Popularity", "bubble size = rating count"),
    unsafe_allow_html=True,
)

# Combine both datasets, dedup by title, take top 100 by rating count
import pandas as pd
df_bubble = pd.concat([df_top_rated, df_most_popular]).drop_duplicates(subset=["title"])
df_bubble = df_bubble.nlargest(100, "rating_count")

fig_bubble = px.scatter(
    df_bubble,
    x="avg_rating",
    y="rating_count",
    size="rating_count",
    color="avg_rating",
    hover_name="title",
    color_continuous_scale=CHART_GRADIENT,
    size_max=45,
    custom_data=["release_year"],
    labels={
        "avg_rating": "Average Rating",
        "rating_count": "Total Ratings",
    },
)
fig_bubble.update_traces(
    hovertemplate="<b>%{hovertext}</b><br>"
                  "Avg Rating: %{x:.2f}<br>"
                  "Ratings: %{y:,.0f}<br>"
                  "Year: %{customdata[0]}<extra></extra>",
    marker=dict(line=dict(width=1, color="rgba(108,99,255,0.25)")),
)

# Label top-10 by rating count
top10 = df_bubble.nlargest(10, "rating_count")
fig_bubble.add_trace(go.Scatter(
    x=top10["avg_rating"],
    y=top10["rating_count"],
    mode="text",
    text=top10["title"].apply(lambda t: t[:25] + "…" if len(t) > 25 else t),
    textposition="top center",
    textfont=dict(size=9, color="#94A3B8", family="Inter, sans-serif"),
    showlegend=False,
    hoverinfo="skip",
))

fig_bubble.update_layout(
    **PLOTLY_LAYOUT,
    height=520,
    coloraxis_colorbar=dict(title="Avg Rating"),
    xaxis=dict(range=[2.5, 5.2]),
)
st.plotly_chart(fig_bubble, use_container_width=True)

st.divider()

# ── Top-N Filter ─────────────────────────────────────────────
top_n = st.slider("Show top N movies", min_value=10, max_value=50, value=30, step=5)

# ── Tabs for Rankings ────────────────────────────────────────
tab_rated, tab_popular = st.tabs(["Top Rated", "Most Popular"])

with tab_rated:
    st.markdown(
        section_header(f"Top {top_n} Highest Rated Movies", "min. 100 ratings"),
        unsafe_allow_html=True,
    )

    df_top_rated_sorted = df_top_rated.sort_values("avg_rating", ascending=True).tail(top_n)

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
        height=max(500, top_n * 22),
        xaxis=dict(range=[3.0, 5.0]),
    )
    st.plotly_chart(fig_rated, use_container_width=True)

with tab_popular:
    st.markdown(
        section_header(f"Top {top_n} Most Popular Movies", "by rating count"),
        unsafe_allow_html=True,
    )

    df_popular_sorted = df_most_popular.sort_values("rating_count", ascending=True).tail(top_n)

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
        height=max(500, top_n * 22),
        coloraxis_colorbar=dict(title="Avg Rating"),
    )
    st.plotly_chart(fig_popular, use_container_width=True)
