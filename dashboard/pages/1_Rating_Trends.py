"""
Page 1 — Rating Trends
=======================
Monthly rating trends (line + bar), year-over-year growth,
and yearly summary table with interactive date filtering.
"""

import streamlit as st
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from dashboard.services.data_loader import load_rating_trends_monthly, load_yearly_summary
from dashboard.config.theme import inject_theme, section_header, callout, PLOTLY_LAYOUT, CHART_GRADIENT, COLORS

st.set_page_config(page_title="Rating Trends | MovieLens", layout="wide")
inject_theme()

st.markdown("# Rating Trends")
st.markdown("How have ratings evolved month-over-month and year-over-year?")
st.markdown("")

# ── Load Data ────────────────────────────────────────────────
df_monthly = load_rating_trends_monthly()
df_yearly = load_yearly_summary()

# ── Year Range Filter ────────────────────────────────────────
all_years = sorted(df_monthly["year"].unique())
if len(all_years) >= 2:
    year_range = st.select_slider(
        "Year Range",
        options=all_years,
        value=(all_years[0], all_years[-1]),
    )
    df_monthly = df_monthly[
        (df_monthly["year"] >= year_range[0]) & (df_monthly["year"] <= year_range[1])
    ]

# ── Monthly Trends — Dual Axis ───────────────────────────────
st.markdown(section_header("Monthly Rating Trends", "volume vs. quality"), unsafe_allow_html=True)

df_monthly["date_label"] = (
    df_monthly["year"].astype(str) + "-" + df_monthly["month"].astype(str).str.zfill(2)
)

fig = make_subplots(specs=[[{"secondary_y": True}]])

# Bar: rating volume with gradient colours
fig.add_trace(
    go.Bar(
        x=df_monthly["date_label"],
        y=df_monthly["rating_count"],
        name="Rating Count",
        marker=dict(
            color=df_monthly["rating_count"],
            colorscale=CHART_GRADIENT,
            line_width=0,
            cornerradius=4,
        ),
    ),
    secondary_y=False,
)

# Line: average rating
fig.add_trace(
    go.Scatter(
        x=df_monthly["date_label"],
        y=df_monthly["avg_rating"],
        name="Avg Rating",
        line=dict(color=COLORS["accent"], width=2.5, shape="spline"),
        mode="lines",
        fill="tonexty",
        fillcolor="rgba(167,139,250,0.06)",
    ),
    secondary_y=True,
)

fig.update_layout(
    **PLOTLY_LAYOUT,
    height=480,
    hovermode="x unified",
)
fig.update_xaxes(rangeslider_visible=True, rangeslider_thickness=0.06)
fig.update_yaxes(title_text="Rating Count", secondary_y=False)
fig.update_yaxes(title_text="Avg Rating", secondary_y=True, range=[2.5, 5.0])

st.plotly_chart(fig, use_container_width=True)

# ── Monthly Users & Movies ───────────────────────────────────
st.markdown(section_header("Monthly Unique Users & Movies"), unsafe_allow_html=True)

col1, col2 = st.columns(2)

with col1:
    fig_users = go.Figure()
    fig_users.add_trace(go.Scatter(
        x=df_monthly["date_label"],
        y=df_monthly["unique_users"],
        fill="tozeroy",
        fillcolor="rgba(108,99,255,0.12)",
        line=dict(color=COLORS["primary"], width=2, shape="spline"),
        name="Unique Users",
        hovertemplate="<b>%{x}</b><br>Users: %{y:,.0f}<extra></extra>",
    ))
    # Mean reference line
    mean_users = df_monthly["unique_users"].mean()
    fig_users.add_hline(
        y=mean_users,
        line=dict(color=COLORS["highlight"], width=1.5, dash="dot"),
        annotation_text=f"Mean: {mean_users:,.0f}",
        annotation_font=dict(color=COLORS["highlight"], size=11, family="Fira Code"),
    )
    fig_users.update_layout(
        **PLOTLY_LAYOUT,
        height=320,
        title=dict(text="Unique Users per Month", font=dict(size=14, family="Fira Code, monospace")),
    )
    fig_users.update_xaxes(rangeslider_visible=True, rangeslider_thickness=0.08)
    st.plotly_chart(fig_users, use_container_width=True)

with col2:
    fig_movies = go.Figure()
    fig_movies.add_trace(go.Scatter(
        x=df_monthly["date_label"],
        y=df_monthly["unique_movies"],
        fill="tozeroy",
        fillcolor="rgba(167,139,250,0.12)",
        line=dict(color=COLORS["accent"], width=2, shape="spline"),
        name="Unique Movies",
        hovertemplate="<b>%{x}</b><br>Movies: %{y:,.0f}<extra></extra>",
    ))
    # Mean reference line
    mean_movies = df_monthly["unique_movies"].mean()
    fig_movies.add_hline(
        y=mean_movies,
        line=dict(color=COLORS["highlight"], width=1.5, dash="dot"),
        annotation_text=f"Mean: {mean_movies:,.0f}",
        annotation_font=dict(color=COLORS["highlight"], size=11, family="Fira Code"),
    )
    fig_movies.update_layout(
        **PLOTLY_LAYOUT,
        height=320,
        title=dict(text="Unique Movies Rated per Month", font=dict(size=14, family="Fira Code, monospace")),
    )
    fig_movies.update_xaxes(rangeslider_visible=True, rangeslider_thickness=0.08)
    st.plotly_chart(fig_movies, use_container_width=True)

# ── Year-over-Year Growth ────────────────────────────────────
st.markdown(section_header("Year-over-Year Growth", "% change in rating volume"), unsafe_allow_html=True)

df_yoy = df_yearly.sort_values("year").copy()
df_yoy["yoy_pct"] = df_yoy["total_ratings"].pct_change() * 100
df_yoy = df_yoy.dropna(subset=["yoy_pct"])

fig_yoy = go.Figure()
fig_yoy.add_trace(go.Bar(
    x=df_yoy["year"].astype(str),
    y=df_yoy["yoy_pct"],
    text=df_yoy["yoy_pct"].apply(lambda v: f"{v:+.1f}%"),
    textposition="outside",
    textfont=dict(family="Fira Code, monospace", size=11, color="#F1F5F9"),
    marker=dict(
        color=["#22C55E" if v >= 0 else "#EF4444" for v in df_yoy["yoy_pct"]],
        line_width=0,
        cornerradius=6,
    ),
    hovertemplate="<b>%{x}</b><br>YoY Change: %{text}<extra></extra>",
))
fig_yoy.update_layout(
    **PLOTLY_LAYOUT,
    height=380,
    showlegend=False,
    xaxis_title="Year",
    yaxis_title="% Change",
)
# Add zero reference line
fig_yoy.add_hline(y=0, line=dict(color=COLORS["muted"], width=1, dash="dash"))
st.plotly_chart(fig_yoy, use_container_width=True)

# ── Yearly Summary Table ─────────────────────────────────────
st.markdown(section_header("Yearly Summary"), unsafe_allow_html=True)

df_display = df_yearly.sort_values("year", ascending=False).copy()
df_display.columns = [
    "Year", "Total Ratings", "Unique Users", "Unique Movies",
    "Avg Rating", "Late Arrivals", "Late Arrival %"
]

max_ratings = df_display["Total Ratings"].max()

st.dataframe(
    df_display,
    use_container_width=True,
    hide_index=True,
    column_config={
        "Year": st.column_config.NumberColumn("Year", format="%d"),
        "Total Ratings": st.column_config.ProgressColumn(
            "Total Ratings",
            min_value=0,
            max_value=int(max_ratings),
            format="%d",
        ),
        "Unique Users": st.column_config.NumberColumn("Unique Users", format="%d"),
        "Unique Movies": st.column_config.NumberColumn("Unique Movies", format="%d"),
        "Avg Rating": st.column_config.NumberColumn("Avg Rating", format="%.2f"),
        "Late Arrivals": st.column_config.NumberColumn("Late Arrivals", format="%d"),
        "Late Arrival %": st.column_config.NumberColumn("Late Arrival %", format="%.3f%%"),
    },
)
