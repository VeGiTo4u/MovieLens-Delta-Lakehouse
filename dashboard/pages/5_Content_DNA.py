"""
Page 5 — Content DNA
=====================
Movie release decade analysis and genome tag relevance
with KPI-first hierarchy and tabbed views.
"""

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from services.data_loader import load_release_decade_analysis, load_top_genome_tags
from config.theme import inject_theme, section_header, kpi_card, callout, PLOTLY_LAYOUT, CHART_GRADIENT, COLORS

st.set_page_config(page_title="Content DNA | MovieLens", layout="wide")
inject_theme()

st.markdown("# Content DNA")
st.markdown("How does era affect ratings? What genome tags define the catalog?")
st.markdown("")

# ── Load Data ────────────────────────────────────────────────
df_decades = load_release_decade_analysis()
df_tags = load_top_genome_tags()

# Filter out very early movies (1800s/1900s/1910s) which have low volume
df_decades = df_decades[df_decades["decade"] >= "1920s"]

# ── Decade KPI Cards (Above Chart — per KPI-first hierarchy) ─
col1, col2, col3 = st.columns(3)
with col1:
    best_decade = df_decades.loc[df_decades["avg_rating"].idxmax()]
    kpi_card("dna", "Best Rated Decade", str(best_decade["decade"]),
             delta=f"Avg: {best_decade['avg_rating']:.2f}",
             delta_color="success")
with col2:
    popular_decade = df_decades.loc[df_decades["total_ratings"].idxmax()]
    kpi_card("ratings", "Most Popular Decade", str(popular_decade["decade"]),
             delta=f"{popular_decade['total_ratings']:,.0f} ratings",
             delta_color="muted")
with col3:
    diverse_decade = df_decades.loc[df_decades["movie_count"].idxmax()]
    kpi_card("movies", "Most Movies", str(diverse_decade["decade"]),
             delta=f"{diverse_decade['movie_count']:,.0f} movies",
             delta_color="muted")

st.divider()

# ── Release Decade Analysis — Tabbed Views ───────────────────
st.markdown(section_header("Ratings by Movie Release Decade", "volume vs. quality"), unsafe_allow_html=True)

tab_bar, tab_scatter = st.tabs(["Volume vs Quality", "Scatter View"])

with tab_bar:
    fig_decade = make_subplots(specs=[[{"secondary_y": True}]])

    fig_decade.add_trace(
        go.Bar(
            x=df_decades["decade"],
            y=df_decades["total_ratings"],
            name="Total Ratings",
            marker=dict(
                color=df_decades["total_ratings"],
                colorscale=CHART_GRADIENT,
                line_width=0,
                cornerradius=4,
            ),
        ),
        secondary_y=False,
    )

    fig_decade.add_trace(
        go.Scatter(
            x=df_decades["decade"],
            y=df_decades["avg_rating"],
            name="Avg Rating",
            mode="lines+markers",
            line=dict(color=COLORS["accent"], width=3, shape="spline"),
            marker=dict(size=9, color="#C4B5FD", line=dict(width=2, color=COLORS["accent"])),
        ),
        secondary_y=True,
    )

    fig_decade.update_layout(
        **PLOTLY_LAYOUT,
        height=480,
        hovermode="x unified",
    )
    fig_decade.update_yaxes(title_text="Total Ratings", secondary_y=False)
    fig_decade.update_yaxes(title_text="Avg Rating", secondary_y=True, range=[2.5, 5.0])
    st.plotly_chart(fig_decade, use_container_width=True)

with tab_scatter:
    fig_decade_scatter = px.scatter(
        df_decades,
        x="avg_rating",
        y="total_ratings",
        size="movie_count",
        color="avg_rating",
        hover_name="decade",
        color_continuous_scale=CHART_GRADIENT,
        size_max=55,
        custom_data=["movie_count"],
        labels={
            "avg_rating": "Average Rating",
            "total_ratings": "Total Ratings",
            "movie_count": "Movies",
        },
    )
    fig_decade_scatter.update_traces(
        hovertemplate="<b>%{hovertext}</b><br>"
                      "Avg Rating: %{x:.2f}<br>"
                      "Ratings: %{y:,.0f}<br>"
                      "Movies: %{customdata[0]:,}<extra></extra>",
        marker=dict(line=dict(width=1, color="rgba(108,99,255,0.25)")),
    )

    # Add decade labels
    fig_decade_scatter.add_traces(
        px.scatter(
            df_decades, x="avg_rating", y="total_ratings", text="decade",
        ).update_traces(
            mode="text",
            textfont=dict(size=10, color="#94A3B8", family="Fira Code"),
            textposition="top center",
            showlegend=False,
            hoverinfo="skip",
        ).data
    )

    fig_decade_scatter.update_layout(
        **PLOTLY_LAYOUT,
        height=480,
        coloraxis_colorbar=dict(title="Avg Rating"),
    )
    st.plotly_chart(fig_decade_scatter, use_container_width=True)

st.divider()

# ── Genome Tags — Dual-Axis: Relevance + Movie Count ────────
st.markdown(section_header("Top 20 Genome Tags by Relevance"), unsafe_allow_html=True)

callout(
    "<strong>Genome relevance</strong> scores measure how strongly a tag "
    "describes the movies in the catalog. A score of 1.0 = perfectly relevant. "
    "The right chart shows total movie count for each tag.",
    kind="info",
)

# Take Top 20 by average relevance
df_tags_top20 = df_tags.sort_values("avg_relevance", ascending=False).head(20)
# Sort ascending for horizontal bar chart display (best at top)
df_tags_plot = df_tags_top20.sort_values("avg_relevance", ascending=True)

fig_tags = make_subplots(
    rows=1, cols=2,
    subplot_titles=("Avg Relevance", "Movie Count"),
    shared_yaxes=True,
    horizontal_spacing=0.1
)

# Col 1: Avg Relevance
fig_tags.add_trace(
    go.Bar(
        x=df_tags_plot["avg_relevance"],
        y=df_tags_plot["tag"],
        orientation="h",
        name="Relevance",
        marker=dict(color=df_tags_plot["avg_relevance"], colorscale=CHART_GRADIENT),
        hovertemplate="<b>%{y}</b><br>Relevance: %{x:.4f}<extra></extra>",
    ),
    row=1, col=1
)

# Col 2: Movie Count
fig_tags.add_trace(
    go.Bar(
        x=df_tags_plot["movie_count"],
        y=df_tags_plot["tag"],
        orientation="h",
        name="Movies",
        marker=dict(color=COLORS["accent"], line_width=0),
        hovertemplate="<b>%{y}</b><br>Movies: %{x:,}<extra></extra>",
    ),
    row=1, col=2
)

fig_tags.update_layout(
    **{**PLOTLY_LAYOUT, "height": 600, "showlegend": False, "margin": dict(l=150, r=40, t=60, b=40)},
)
fig_tags.update_xaxes(title_text="Relevance", row=1, col=1)
fig_tags.update_xaxes(title_text="Movie Count", row=1, col=2)

st.plotly_chart(fig_tags, use_container_width=True)
