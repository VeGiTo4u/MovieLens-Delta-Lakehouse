"""
Page 5 — Content DNA
=====================
Movie release decade analysis and genome tag relevance.
"""

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from data_loader import load_release_decade_analysis, load_top_genome_tags
from theme import inject_theme, section_header, PLOTLY_LAYOUT, CHART_GRADIENT, COLORS

st.set_page_config(page_title="Content DNA | MovieLens", page_icon="M", layout="wide")
inject_theme()

st.markdown("# Content DNA")
st.markdown("How does era affect ratings? What genome tags define the catalog?")
st.markdown("")

# ── Load Data ────────────────────────────────────────────────
df_decades = load_release_decade_analysis()
df_tags = load_top_genome_tags()

# Filter out very early movies (1800s/1900s/1910s) which have low volume
df_decades = df_decades[df_decades["decade"] >= "1920s"]

# ── Release Decade Analysis ──────────────────────────────────
st.markdown(section_header("Ratings by Movie Release Decade", "volume vs. quality"), unsafe_allow_html=True)

fig_decade = make_subplots(specs=[[{"secondary_y": True}]])

fig_decade.add_trace(
    go.Bar(
        x=df_decades["decade"],
        y=df_decades["total_ratings"],
        name="Total Ratings",
        marker_color="rgba(108,99,255,0.50)",
        marker_line_width=0,
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
    height=450,
    hovermode="x unified",
)
fig_decade.update_yaxes(title_text="Total Ratings", secondary_y=False)
fig_decade.update_yaxes(title_text="Avg Rating", secondary_y=True, range=[2.5, 5.0])
st.plotly_chart(fig_decade, use_container_width=True)

# Decade stats cards
col1, col2, col3 = st.columns(3)
with col1:
    best_decade = df_decades.loc[df_decades["avg_rating"].idxmax()]
    st.metric("Best Rated Decade", best_decade["decade"], f"{best_decade['avg_rating']:.2f} avg")
with col2:
    popular_decade = df_decades.loc[df_decades["total_ratings"].idxmax()]
    st.metric("Most Popular Decade", popular_decade["decade"], f"{popular_decade['total_ratings']:,.0f} ratings")
with col3:
    diverse_decade = df_decades.loc[df_decades["movie_count"].idxmax()]
    st.metric("Most Movies", diverse_decade["decade"], f"{diverse_decade['movie_count']:,.0f} movies")

st.markdown("")

# ── Genome Tags ──────────────────────────────────────────────
st.markdown(section_header("Top 20 Genome Tags by Relevance"), unsafe_allow_html=True)

df_tags_sorted = df_tags.sort_values("avg_relevance", ascending=True)

fig_tags = px.bar(
    df_tags_sorted,
    x="avg_relevance",
    y="tag",
    orientation="h",
    color="avg_relevance",
    color_continuous_scale=CHART_GRADIENT,
    custom_data=["movie_count"],
    labels={"avg_relevance": "Avg Relevance", "tag": ""},
)
fig_tags.update_traces(
    hovertemplate="<b>%{y}</b><br>Avg Relevance: %{x:.4f}<br>Movies: %{customdata[0]:,}<extra></extra>",
    marker_line_width=0,
    marker_cornerradius=4,
)
fig_tags.update_layout(
    **{**PLOTLY_LAYOUT, "margin": dict(t=20, b=30, l=200, r=20)},
    coloraxis_showscale=False,
    height=550,
)
st.plotly_chart(fig_tags, use_container_width=True)
