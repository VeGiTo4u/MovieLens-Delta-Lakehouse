"""
Page 4 — User Engagement
=========================
User activity distribution and rating value breakdown.
"""

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from data_loader import load_user_activity_distribution, load_rating_distribution

st.set_page_config(page_title="User Engagement | MovieLens", page_icon="👥", layout="wide")

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

st.markdown("# 👥 User Engagement")
st.markdown("How active are users? What ratings do they give?")
st.markdown("")

# ── Load Data ────────────────────────────────────────────────
df_activity = load_user_activity_distribution()
df_rating = load_rating_distribution()

# ── Activity Distribution ────────────────────────────────────
st.markdown('<p class="section-header">User Activity Distribution</p>', unsafe_allow_html=True)

# Clean bucket labels for display
df_activity["bucket_label"] = df_activity["activity_bucket"].str.replace(r"^\d+\s*—\s*", "", regex=True)

col1, col2 = st.columns([3, 2])

with col1:
    fig_bar = px.bar(
        df_activity,
        x="bucket_label",
        y="user_count",
        text="pct_of_users",
        color="user_count",
        color_continuous_scale=["#2D2B55", "#6C63FF", "#A78BFA"],
        labels={"bucket_label": "Ratings per User", "user_count": "Users"},
    )
    fig_bar.update_traces(
        texttemplate="%{text:.1f}%",
        textposition="outside",
        marker_line_width=0,
    )
    fig_bar.update_layout(
        template="plotly_dark",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        coloraxis_showscale=False,
        height=400,
        margin=dict(t=30, b=60),
        font=dict(family="Inter, sans-serif"),
    )
    st.plotly_chart(fig_bar, use_container_width=True)

with col2:
    fig_pie = px.pie(
        df_activity,
        values="user_count",
        names="bucket_label",
        color_discrete_sequence=["#1E1B4B", "#312E81", "#4338CA", "#6C63FF", "#A78BFA", "#C4B5FD"],
        hole=0.45,
    )
    fig_pie.update_traces(
        textposition="inside",
        textinfo="percent+label",
        textfont_size=11,
    )
    fig_pie.update_layout(
        template="plotly_dark",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        height=400,
        margin=dict(t=30, b=30),
        showlegend=False,
        font=dict(family="Inter, sans-serif"),
    )
    st.plotly_chart(fig_pie, use_container_width=True)

# ── Rating Value Distribution ────────────────────────────────
st.markdown('<p class="section-header">Rating Value Distribution</p>', unsafe_allow_html=True)

fig_rating = go.Figure()

fig_rating.add_trace(go.Bar(
    x=df_rating["rating"].astype(str),
    y=df_rating["rating_count"],
    text=df_rating["pct_of_total"].apply(lambda x: f"{x:.1f}%"),
    textposition="outside",
    marker=dict(
        color=df_rating["rating_count"],
        colorscale=["#2D2B55", "#4338CA", "#6C63FF", "#A78BFA", "#C4B5FD"],
        line_width=0,
    ),
))

fig_rating.update_layout(
    template="plotly_dark",
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    height=400,
    margin=dict(t=30, b=60),
    xaxis_title="Rating Value",
    yaxis_title="Count",
    font=dict(family="Inter, sans-serif"),
)
st.plotly_chart(fig_rating, use_container_width=True)
