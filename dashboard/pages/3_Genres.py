"""
Page 3 — Genres
================
Genre performance scatter, breakdown charts, genre popularity
trends over time (stacked area + heatmap).
"""

import streamlit as st
import plotly.express as px
from dashboard.services.data_loader import load_genre_performance, load_genre_trends_yearly
from dashboard.config.theme import inject_theme, section_header, callout, PLOTLY_LAYOUT, CHART_GRADIENT, COLORS

st.set_page_config(page_title="Genres | MovieLens", layout="wide")
inject_theme()

st.markdown("# Genre Analysis")
st.markdown("How do genres compare in popularity and quality? How have they evolved over time?")
st.markdown("")

# ── Load Data ────────────────────────────────────────────────
df_genre = load_genre_performance()
df_genre_trends = load_genre_trends_yearly()

# ── Genre Performance — Scatter Overview ─────────────────────
st.markdown(section_header("Genre Performance Overview", "bubble size = unique movies"), unsafe_allow_html=True)

fig_scatter = px.scatter(
    df_genre,
    x="avg_rating",
    y="rating_count",
    size="unique_movies",
    color="avg_rating",
    hover_name="genre_name",
    color_continuous_scale=CHART_GRADIENT,
    size_max=50,
    custom_data=["unique_movies", "unique_users"],
    labels={
        "avg_rating": "Average Rating",
        "rating_count": "Total Ratings",
        "unique_movies": "Movies",
    },
)
fig_scatter.update_traces(
    hovertemplate="<b>%{hovertext}</b><br>"
                  "Avg Rating: %{x:.2f}<br>"
                  "Ratings: %{y:,.0f}<br>"
                  "Movies: %{customdata[0]:,}<br>"
                  "Users: %{customdata[1]:,}<extra></extra>",
    marker=dict(line=dict(width=1, color="rgba(108,99,255,0.25)")),
    textposition="top center",
)

# Add genre name labels
fig_scatter.add_traces(
    px.scatter(
        df_genre,
        x="avg_rating",
        y="rating_count",
        text="genre_name",
    ).update_traces(
        mode="text",
        textfont=dict(size=9, color="#94A3B8", family="Inter"),
        textposition="top center",
        showlegend=False,
        hoverinfo="skip",
    ).data
)

fig_scatter.update_layout(
    **PLOTLY_LAYOUT,
    height=520,
    coloraxis_colorbar=dict(title="Avg Rating"),
)
st.plotly_chart(fig_scatter, use_container_width=True)

st.divider()

# ── Detailed Chart Tabs — Bars vs Scatter ────────────────────
tab_bars, tab_detail = st.tabs(["Rating Breakdown", "By Rating Count"])

with tab_bars:
    st.markdown(section_header("Ratings by Genre"), unsafe_allow_html=True)

    df_sorted = df_genre.sort_values("rating_count", ascending=True)
    fig_count = px.bar(
        df_sorted,
        x="rating_count",
        y="genre_name",
        orientation="h",
        color="avg_rating",
        color_continuous_scale=CHART_GRADIENT,
        custom_data=["avg_rating", "unique_movies", "unique_users"],
        labels={"rating_count": "Total Ratings", "genre_name": "", "avg_rating": "Avg Rating"},
    )
    fig_count.update_traces(
        hovertemplate="<b>%{y}</b><br>Ratings: %{x:,}<br>Avg Rating: %{customdata[0]:.2f}<br>Movies: %{customdata[1]:,}<br>Users: %{customdata[2]:,}<extra></extra>",
        marker_line_width=0,
        marker_cornerradius=4,
    )
    fig_count.update_layout(
        **{**PLOTLY_LAYOUT, "margin": dict(t=30, b=30, l=120, r=20)},
        height=550,
        coloraxis_colorbar=dict(title="Avg Rating"),
    )
    st.plotly_chart(fig_count, use_container_width=True)

with tab_detail:
    st.markdown(section_header("Average Rating by Genre"), unsafe_allow_html=True)

    df_avg_sorted = df_genre.sort_values("avg_rating", ascending=True)
    fig_avg = px.bar(
        df_avg_sorted,
        x="avg_rating",
        y="genre_name",
        orientation="h",
        color="rating_count",
        color_continuous_scale=CHART_GRADIENT,
        custom_data=["rating_count", "unique_movies"],
        labels={"avg_rating": "Average Rating", "genre_name": "", "rating_count": "Ratings"},
    )
    fig_avg.update_traces(
        hovertemplate="<b>%{y}</b><br>Avg Rating: %{x:.2f}<br>Ratings: %{customdata[0]:,}<br>Movies: %{customdata[1]:,}<extra></extra>",
        marker_line_width=0,
        marker_cornerradius=4,
    )
    fig_avg.update_layout(
        **{**PLOTLY_LAYOUT, "margin": dict(t=30, b=30, l=120, r=20)},
        height=550,
        coloraxis_colorbar=dict(title="Count"),
    )
    st.plotly_chart(fig_avg, use_container_width=True)

st.divider()

# ── Genre Trends — Tabs: Stacked Area vs Heatmap ────────────
st.markdown(section_header("Genre Popularity Over Time"), unsafe_allow_html=True)

tab_area, tab_heat = st.tabs(["Stacked Area (Top 5)", "Full Heatmap"])

with tab_area:
    # Get top-5 genres by total ratings
    top5_genres = df_genre.nlargest(5, "rating_count")["genre_name"].tolist()
    df_top5 = df_genre_trends[df_genre_trends["genre_name"].isin(top5_genres)]

    fig_area = px.area(
        df_top5,
        x="year",
        y="rating_count",
        color="genre_name",
        color_discrete_sequence=["#6C63FF", "#A78BFA", "#C4B5FD", "#818CF8", "#4338CA"],
        labels={"rating_count": "Ratings", "year": "Year", "genre_name": "Genre"},
    )
    fig_area.update_layout(
        **{k: v for k, v in PLOTLY_LAYOUT.items() if k != "legend"},
        height=480,
        hovermode="x unified",
        legend=dict(
            orientation="h", y=1.08, x=0.5, xanchor="center",
            font=dict(size=11, color="#94A3B8"),
        ),
    )
    fig_area.update_traces(
        line=dict(width=1.5),
        opacity=0.85,
    )
    st.plotly_chart(fig_area, use_container_width=True)

with tab_heat:
    pivot = df_genre_trends.pivot_table(
        index="genre_name", columns="year", values="rating_count", fill_value=0
    )

    fig_heat = px.imshow(
        pivot,
        color_continuous_scale=["#0A0E17", "#1E1B4B", "#4338CA", "#6C63FF", "#A78BFA", "#C4B5FD"],
        labels=dict(x="Year", y="Genre", color="Ratings"),
        aspect="auto",
        text_auto=".2s",
        zmin=0,
        zmax=int(pivot.values.max()),
    )
    fig_heat.update_traces(
        hovertemplate="<b>%{y}</b><br>%{x}: %{z:,.0f} ratings<extra></extra>",
        textfont=dict(size=9),
    )
    fig_heat.update_layout(
        **{**PLOTLY_LAYOUT, "margin": dict(t=30, b=30, l=120, r=20)},
        height=520,
        coloraxis_colorbar=dict(title="Ratings"),
    )
    st.plotly_chart(fig_heat, use_container_width=True)
