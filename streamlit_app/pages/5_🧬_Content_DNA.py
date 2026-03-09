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

st.set_page_config(page_title="Content DNA | MovieLens", page_icon="🧬", layout="wide")

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

st.markdown("# 🧬 Content DNA")
st.markdown("How does era affect ratings? What genome tags define the catalog?")
st.markdown("")

# ── Load Data ────────────────────────────────────────────────
df_decades = load_release_decade_analysis()
df_tags = load_top_genome_tags()

# Filter out very early movies (1800s/1900s/1910s) which have low volume 
# and skew the "Best Rated Decade" metric (e.g., 1940s appearing artificially high).
df_decades = df_decades[df_decades["decade"] >= "1920s"]

# ── Release Decade Analysis ──────────────────────────────────
st.markdown('<p class="section-header">Ratings by Movie Release Decade</p>', unsafe_allow_html=True)

fig_decade = make_subplots(specs=[[{"secondary_y": True}]])

fig_decade.add_trace(
    go.Bar(
        x=df_decades["decade"],
        y=df_decades["total_ratings"],
        name="Total Ratings",
        marker_color="rgba(108,99,255,0.6)",
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
        line=dict(color="#A78BFA", width=3),
        marker=dict(size=8, color="#C4B5FD"),
    ),
    secondary_y=True,
)

fig_decade.update_layout(
    template="plotly_dark",
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    height=450,
    margin=dict(t=30, b=60),
    legend=dict(orientation="h", y=1.08, x=0.5, xanchor="center"),
    font=dict(family="Inter, sans-serif"),
    hovermode="x unified",
)
fig_decade.update_yaxes(title_text="Total Ratings", secondary_y=False)
fig_decade.update_yaxes(title_text="Avg Rating", secondary_y=True, range=[2.5, 5.0])
st.plotly_chart(fig_decade, use_container_width=True)

# Decade stats cards
col1, col2, col3 = st.columns(3)
with col1:
    best_decade = df_decades.loc[df_decades["avg_rating"].idxmax()]
    st.metric("🏆 Best Rated Decade", best_decade["decade"], f"{best_decade['avg_rating']:.2f} ★")
with col2:
    popular_decade = df_decades.loc[df_decades["total_ratings"].idxmax()]
    st.metric("🔥 Most Popular Decade", popular_decade["decade"], f"{popular_decade['total_ratings']:,.0f} ratings")
with col3:
    diverse_decade = df_decades.loc[df_decades["movie_count"].idxmax()]
    st.metric("🎬 Most Movies", diverse_decade["decade"], f"{diverse_decade['movie_count']:,.0f} movies")

st.markdown("")

# ── Genome Tags ──────────────────────────────────────────────
st.markdown('<p class="section-header">Top 20 Genome Tags by Relevance</p>', unsafe_allow_html=True)

df_tags_sorted = df_tags.sort_values("avg_relevance", ascending=True)

fig_tags = px.bar(
    df_tags_sorted,
    x="avg_relevance",
    y="tag",
    orientation="h",
    color="avg_relevance",
    color_continuous_scale=["#1E1B4B", "#4338CA", "#6C63FF", "#A78BFA"],
    custom_data=["movie_count"],
    labels={"avg_relevance": "Avg Relevance", "tag": ""},
)
fig_tags.update_traces(
    hovertemplate="<b>%{y}</b><br>Avg Relevance: %{x:.4f}<br>Movies: %{customdata[0]:,}<extra></extra>",
    marker_line_width=0,
)
fig_tags.update_layout(
    template="plotly_dark",
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    coloraxis_showscale=False,
    height=550,
    margin=dict(t=20, b=30, l=200),
    font=dict(family="Inter, sans-serif"),
)
st.plotly_chart(fig_tags, use_container_width=True)
