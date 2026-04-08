"""
Page 4 — User Engagement
=========================
User activity distribution and rating value breakdown with
engagement stats alongside visualizations.
"""

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from dashboard.services.data_loader import load_user_activity_distribution, load_rating_distribution
from dashboard.config.theme import inject_theme, section_header, kpi_card, callout, PLOTLY_LAYOUT, CHART_GRADIENT, CHART_COLORS, COLORS

st.set_page_config(page_title="User Engagement | MovieLens", layout="wide")
inject_theme()

st.markdown("# User Engagement")
st.markdown("How active are users? What ratings do they give?")
st.markdown("")

# ── Load Data ────────────────────────────────────────────────
df_activity = load_user_activity_distribution()
df_rating = load_rating_distribution()

# ── Engagement Stat Cards ────────────────────────────────────
total_users = int(df_activity["user_count"].sum())
total_ratings = int(df_rating["rating_count"].sum())
avg_ratings_per_user = total_ratings / total_users if total_users > 0 else 0

# Find the most common activity bucket
top_bucket = df_activity.sort_values("user_count", ascending=False).iloc[0]

col_s1, col_s2, col_s3 = st.columns(3)
with col_s1:
    kpi_card("users", "Total Users", f"{total_users:,}")
with col_s2:
    kpi_card("ratings", "Avg Ratings / User", f"{avg_ratings_per_user:.1f}")
with col_s3:
    kpi_card("activity", "Most Common Bucket",
             str(top_bucket.get("activity_bucket", "N/A")),
             delta=f"{top_bucket.get('pct_of_users', 0):.1f}% of users",
             delta_color="muted")

st.divider()

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
        marker_line=dict(width=1, color="rgba(108,99,255,0.3)"),
        marker_cornerradius=6,
    )
    fig_bar.update_layout(
        **PLOTLY_LAYOUT,
        coloraxis_showscale=False,
        height=420,
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
        height=420,
        showlegend=False,
        annotations=[dict(
            text=f"<b>{total_users:,}</b>",
            x=0.5, y=0.5,
            font=dict(size=18, family="Fira Code, monospace", color="#F1F5F9"),
            showarrow=False,
        )],
    )
    st.plotly_chart(fig_pie, use_container_width=True)

st.divider()

# ── Rating Value Distribution ────────────────────────────────
st.markdown(section_header("Rating Value Distribution"), unsafe_allow_html=True)

# Cumulative % for waterfall insight
df_rating_sorted = df_rating.sort_values("rating", ascending=True).copy()
df_rating_sorted["cumulative_pct"] = df_rating_sorted["pct_of_total"].cumsum()

col_chart, col_insight = st.columns([3, 1])

with col_chart:
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
            line=dict(width=1, color="rgba(108,99,255,0.3)"),
            cornerradius=6,
        ),
        hovertemplate="<b>Rating %{x}</b><br>Count: %{y:,.0f}<br>%{text}<extra></extra>",
        name="Ratings",
    ))

    fig_rating.update_layout(
        **PLOTLY_LAYOUT,
        height=420,
        showlegend=False,
        xaxis_title="Rating Value",
        yaxis_title="Count",
    )
    st.plotly_chart(fig_rating, use_container_width=True)

with col_insight:
    # Find the most common rating
    most_common = df_rating.loc[df_rating["rating_count"].idxmax()]
    st.metric("Most Common Rating", f"{most_common['rating']}")
    st.metric("Count", f"{most_common['rating_count']:,.0f}")
    st.metric("Share", f"{most_common['pct_of_total']:.1f}%")
    callout(
        f"Rating <strong>{most_common['rating']}</strong> accounts for "
        f"<strong>{most_common['pct_of_total']:.1f}%</strong> of all ratings.",
        kind="info",
    )
