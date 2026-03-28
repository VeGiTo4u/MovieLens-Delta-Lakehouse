"""
Page 4 — User Engagement
=========================
User activity distribution and rating value breakdown.
"""

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from services.data_loader import load_user_activity_distribution, load_rating_distribution
from config.theme import inject_theme, section_header, PLOTLY_LAYOUT, CHART_GRADIENT, CHART_COLORS

st.set_page_config(page_title="User Engagement | MovieLens", page_icon="M", layout="wide")
inject_theme()

st.markdown("# User Engagement")
st.markdown("How active are users? What ratings do they give?")
st.markdown("")

# ── Load Data ────────────────────────────────────────────────
df_activity = load_user_activity_distribution()
df_rating = load_rating_distribution()

# ── Activity Distribution ────────────────────────────────────
st.markdown(section_header("User Activity Distribution"), unsafe_allow_html=True)

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
        color_continuous_scale=CHART_GRADIENT,
        labels={"bucket_label": "Ratings per User", "user_count": "Users"},
    )
    fig_bar.update_traces(
        texttemplate="%{text:.1f}%",
        textposition="outside",
        textfont=dict(family="Fira Code, monospace", size=11, color="#F1F5F9"),
        marker_line_width=0,
        marker_cornerradius=6,
    )
    fig_bar.update_layout(
        **PLOTLY_LAYOUT,
        coloraxis_showscale=False,
        height=400,
    )
    st.plotly_chart(fig_bar, use_container_width=True)

with col2:
    donut_colors = ["#1E1B4B", "#312E81", "#4338CA", "#6C63FF", "#A78BFA", "#C4B5FD"]
    fig_pie = px.pie(
        df_activity,
        values="user_count",
        names="bucket_label",
        color_discrete_sequence=donut_colors,
        hole=0.5,
    )
    fig_pie.update_traces(
        textposition="inside",
        textinfo="percent+label",
        textfont=dict(size=11, family="Inter, sans-serif"),
        marker=dict(line=dict(color="#0A0E17", width=2)),
    )
    fig_pie.update_layout(
        **PLOTLY_LAYOUT,
        height=400,
        showlegend=False,
        annotations=[dict(
            text="Users",
            x=0.5, y=0.5,
            font=dict(size=16, family="Fira Code, monospace", color="#94A3B8"),
            showarrow=False,
        )],
    )
    st.plotly_chart(fig_pie, use_container_width=True)

# ── Rating Value Distribution ────────────────────────────────
st.markdown(section_header("Rating Value Distribution"), unsafe_allow_html=True)

fig_rating = go.Figure()

fig_rating.add_trace(go.Bar(
    x=df_rating["rating"].astype(str),
    y=df_rating["rating_count"],
    text=df_rating["pct_of_total"].apply(lambda x: f"{x:.1f}%"),
    textposition="outside",
    textfont=dict(family="Fira Code, monospace", size=11, color="#F1F5F9"),
    marker=dict(
        color=df_rating["rating_count"],
        colorscale=CHART_GRADIENT,
        line_width=0,
        cornerradius=6,
    ),
))

fig_rating.update_layout(
    **PLOTLY_LAYOUT,
    height=400,
    xaxis_title="Rating Value",
    yaxis_title="Count",
)
st.plotly_chart(fig_rating, use_container_width=True)
