"""
theme.py — Centralized Design System for MovieLens Dashboard
=============================================================
Import `inject_theme()` on every page to get consistent styling.
Import `PLOTLY_LAYOUT` as the base for all Plotly charts.
"""

import streamlit as st

# ─── Design Tokens ──────────────────────────────────────────
COLORS = {
    "primary":    "#6C63FF",
    "accent":     "#A78BFA",
    "highlight":  "#F59E0B",
    "bg":         "#0A0E17",
    "surface":    "#111827",
    "surface2":   "#1A1F2E",
    "border":     "rgba(108,99,255,0.20)",
    "text":       "#F1F5F9",
    "muted":      "#94A3B8",
    "success":    "#22C55E",
    "danger":     "#EF4444",
}

CHART_COLORS = ["#6C63FF", "#A78BFA", "#C4B5FD", "#818CF8", "#4338CA"]
CHART_GRADIENT = ["#1E1B4B", "#312E81", "#4338CA", "#6C63FF", "#A78BFA", "#C4B5FD"]

# ─── Plotly Base Layout ─────────────────────────────────────
PLOTLY_LAYOUT = dict(
    template="plotly_dark",
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font=dict(family="Inter, sans-serif", color="#F1F5F9", size=13),
    margin=dict(t=30, b=40, l=60, r=20),
    hoverlabel=dict(
        bgcolor="#1A1F2E",
        bordercolor="rgba(108,99,255,0.3)",
        font=dict(family="Inter, sans-serif", color="#F1F5F9", size=13),
    ),
    legend=dict(
        orientation="h",
        y=1.08,
        x=0.5,
        xanchor="center",
        font=dict(size=12, color="#94A3B8"),
    ),
)


def section_header(text: str, subtitle: str = "") -> str:
    """Return styled section header HTML."""
    sub = f'<span class="section-subtitle">{subtitle}</span>' if subtitle else ""
    return f'<div class="section-header">{text}{sub}</div>'


def inject_theme():
    """Inject full CSS design system. Call once per page."""
    st.markdown("""
    <style>
    /* ── Google Fonts ─────────────────────────────────── */
    @import url('https://fonts.googleapis.com/css2?family=Fira+Code:wght@400;500;600;700&family=Inter:wght@300;400;500;600;700&display=swap');

    /* ── Root Variables ───────────────────────────────── */
    :root {
        --primary: #6C63FF;
        --accent: #A78BFA;
        --highlight: #F59E0B;
        --bg: #0A0E17;
        --surface: #111827;
        --surface2: #1A1F2E;
        --border: rgba(108,99,255,0.20);
        --text: #F1F5F9;
        --muted: #94A3B8;
    }

    /* ── Global Body ──────────────────────────────────── */
    .stApp {
        background: var(--bg) !important;
    }
    html, body, [data-testid="stAppViewContainer"] {
        font-family: 'Inter', sans-serif !important;
        color: var(--text) !important;
    }

    /* ── Scrollbar ────────────────────────────────────── */
    ::-webkit-scrollbar { width: 6px; height: 6px; }
    ::-webkit-scrollbar-track { background: var(--bg); }
    ::-webkit-scrollbar-thumb {
        background: rgba(108,99,255,0.3);
        border-radius: 3px;
    }
    ::-webkit-scrollbar-thumb:hover { background: rgba(108,99,255,0.5); }

    /* ── Sidebar ──────────────────────────────────────── */
    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #0D1117 0%, var(--surface2) 100%) !important;
        border-right: 1px solid var(--border) !important;
    }
    [data-testid="stSidebar"] .stMarkdown h2 {
        font-family: 'Fira Code', monospace !important;
        font-size: 1.15rem !important;
        font-weight: 600 !important;
        background: linear-gradient(135deg, var(--primary), var(--accent));
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        background-clip: text;
        letter-spacing: -0.02em;
    }
    [data-testid="stSidebar"] hr {
        border: none;
        border-top: 1px solid var(--border);
        margin: 0.8rem 0;
    }
    [data-testid="stSidebar"] .stCaption p {
        color: var(--muted) !important;
        font-size: 0.75rem !important;
        letter-spacing: 0.03em;
    }

    /* Badge-style tech labels in sidebar */
    .sidebar-badge {
        display: inline-block;
        background: rgba(108,99,255,0.12);
        color: var(--accent);
        font-family: 'Fira Code', monospace;
        font-size: 0.68rem;
        padding: 2px 8px;
        border-radius: 4px;
        border: 1px solid rgba(108,99,255,0.15);
        margin: 2px 3px 2px 0;
        letter-spacing: 0.02em;
    }

    /* ── Page Header ──────────────────────────────────── */
    .stApp h1 {
        font-family: 'Fira Code', monospace !important;
        font-weight: 700 !important;
        font-size: 1.85rem !important;
        background: linear-gradient(135deg, #FFFFFF 0%, var(--accent) 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        background-clip: text;
        letter-spacing: -0.03em;
        margin-bottom: 0.15rem !important;
    }

    /* ── Section Headers ──────────────────────────────── */
    .section-header {
        font-family: 'Fira Code', monospace;
        font-size: 1.1rem;
        font-weight: 600;
        color: var(--text);
        margin: 2rem 0 0.75rem 0;
        padding-bottom: 0.5rem;
        border-bottom: 2px solid var(--border);
        display: flex;
        align-items: baseline;
        gap: 10px;
    }
    .section-subtitle {
        font-family: 'Inter', sans-serif;
        font-size: 0.78rem;
        color: var(--muted);
        font-weight: 400;
    }

    /* ── Metric Cards (Glassmorphism) ─────────────────── */
    div[data-testid="stMetric"] {
        background: linear-gradient(135deg, rgba(108,99,255,0.10) 0%, rgba(17,24,39,0.80) 100%);
        border: 1px solid var(--border);
        border-radius: 14px;
        padding: 20px 22px;
        backdrop-filter: blur(16px);
        -webkit-backdrop-filter: blur(16px);
        transition: all 0.25s ease;
        position: relative;
        overflow: hidden;
    }
    div[data-testid="stMetric"]::before {
        content: '';
        position: absolute;
        top: 0; left: 0; right: 0;
        height: 2px;
        background: linear-gradient(90deg, var(--primary), var(--accent), transparent);
        opacity: 0;
        transition: opacity 0.25s ease;
    }
    div[data-testid="stMetric"]:hover {
        transform: translateY(-3px);
        box-shadow: 0 8px 30px rgba(108,99,255,0.18), 0 0 0 1px rgba(108,99,255,0.15);
        border-color: rgba(108,99,255,0.35);
    }
    div[data-testid="stMetric"]:hover::before {
        opacity: 1;
    }
    div[data-testid="stMetric"] label {
        color: var(--muted) !important;
        font-family: 'Inter', sans-serif !important;
        font-size: 0.75rem !important;
        font-weight: 500 !important;
        text-transform: uppercase;
        letter-spacing: 0.08em;
    }
    div[data-testid="stMetric"] [data-testid="stMetricValue"] {
        color: var(--text) !important;
        font-family: 'Fira Code', monospace !important;
        font-weight: 700 !important;
        font-size: 1.6rem !important;
    }
    div[data-testid="stMetric"] [data-testid="stMetricDelta"] {
        font-family: 'Fira Code', monospace !important;
        font-size: 0.78rem !important;
    }

    /* ── DataFrames / Tables ──────────────────────────── */
    [data-testid="stDataFrame"] {
        border-radius: 12px;
        overflow: hidden;
        border: 1px solid var(--border);
    }
    [data-testid="stDataFrame"] table {
        font-family: 'Inter', sans-serif !important;
        font-size: 0.85rem !important;
    }
    [data-testid="stDataFrame"] th {
        font-family: 'Fira Code', monospace !important;
        font-size: 0.75rem !important;
        text-transform: uppercase !important;
        letter-spacing: 0.06em !important;
        color: var(--muted) !important;
        background: var(--surface) !important;
    }

    /* ── Nav Links ────────────────────────────────────── */
    [data-testid="stSidebar"] a[data-testid="stSidebarNavLink"] {
        font-family: 'Inter', sans-serif !important;
        font-size: 0.88rem !important;
        padding: 0.5rem 0.75rem !important;
        border-radius: 8px !important;
        transition: all 0.2s ease !important;
    }
    [data-testid="stSidebar"] a[data-testid="stSidebarNavLink"]:hover {
        background: rgba(108,99,255,0.10) !important;
    }
    [data-testid="stSidebar"] a[data-testid="stSidebarNavLink"][aria-current="page"] {
        background: rgba(108,99,255,0.15) !important;
        border-left: 3px solid var(--primary) !important;
    }

    /* ── Plotly Charts Container ──────────────────────── */
    [data-testid="stPlotlyChart"] {
        border-radius: 12px;
        overflow: hidden;
        border: 1px solid var(--border);
        background: var(--surface);
        padding: 8px;
    }

    /* ── Hide Streamlit Branding ──────────────────────── */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header[data-testid="stHeader"] {
        background: var(--bg) !important;
        border-bottom: 1px solid var(--border) !important;
    }

    /* ── Tabs ─────────────────────────────────────────── */
    .stTabs [data-baseweb="tab-list"] {
        gap: 4px;
    }
    .stTabs [data-baseweb="tab"] {
        font-family: 'Inter', sans-serif !important;
        font-size: 0.85rem !important;
        font-weight: 500 !important;
        border-radius: 8px 8px 0 0 !important;
        padding: 8px 16px !important;
        color: var(--muted) !important;
        transition: all 0.2s ease !important;
    }
    .stTabs [data-baseweb="tab"]:hover {
        color: var(--text) !important;
        background: rgba(108,99,255,0.08) !important;
    }
    .stTabs [aria-selected="true"] {
        color: var(--text) !important;
        border-bottom: 2px solid var(--primary) !important;
    }

    /* ── Reduced Motion ───────────────────────────────── */
    @media (prefers-reduced-motion: reduce) {
        div[data-testid="stMetric"],
        div[data-testid="stMetric"]::before,
        [data-testid="stSidebar"] a {
            transition: none !important;
        }
    }
    </style>
    """, unsafe_allow_html=True)


def sidebar_badges(labels: list[str]):
    """Render a row of tech-stack badges in the sidebar."""
    html = " ".join(f'<span class="sidebar-badge">{lbl}</span>' for lbl in labels)
    st.markdown(html, unsafe_allow_html=True)
