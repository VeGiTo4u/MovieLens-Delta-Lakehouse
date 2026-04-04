"""
theme.py — Centralized Design System for MovieLens Dashboard
=============================================================
Import `inject_theme()` on every page to get consistent styling.
Import `PLOTLY_LAYOUT` as the base for all Plotly charts.
Import `kpi_card()` / `callout()` for premium glassmorphic components.
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


# ─── SVG Icons — Lucide Icons Mapping ───────────────────────
ICONS = {
    "ratings":   '<svg xmlns="http://www.w3.org/2000/svg" width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" class="lucide lucide-bar-chart"><line x1="12" x2="12" y1="20" y2="10"/><line x1="18" x2="18" y1="20" y2="4"/><line x1="6" x2="6" y1="20" y2="16"/></svg>',
    "users":     '<svg xmlns="http://www.w3.org/2000/svg" width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" class="lucide lucide-users"><path d="M16 21v-2a4 4 0 0 0-4-4H6a4 4 0 0 0-4 4v2"/><circle cx="9" cy="7" r="4"/><path d="M22 21v-2a4 4 0 0 0-3-3.87"/><path d="M16 3.13a4 4 0 0 1 0 7.75"/></svg>',
    "movies":    '<svg xmlns="http://www.w3.org/2000/svg" width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" class="lucide lucide-film"><rect width="18" height="18" x="3" y="3" rx="2"/><path d="M7 3v18"/><path d="M17 3v18"/><path d="M3 7h4"/><path d="M3 12h18"/><path d="M3 17h4"/><path d="M17 12h4"/><path d="M17 7h4"/><path d="M17 17h4"/></svg>',
    "star":      '<svg xmlns="http://www.w3.org/2000/svg" width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" class="lucide lucide-star"><polygon points="12 2 15.09 8.26 22 9.27 17 14.14 18.18 21.02 12 17.77 5.82 21.02 7 14.14 2 9.27 8.91 8.26 12 2"/></svg>',
    "trending-up": '<svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" class="lucide lucide-trending-up" style="display:inline; vertical-align:middle; margin-right:4px;"><polyline points="22 7 13.5 15.5 8.5 10.5 2 17"/><polyline points="16 7 22 7 22 13"/></svg>',
    "trending-down": '<svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" class="lucide lucide-trending-down" style="display:inline; vertical-align:middle; margin-right:4px;"><polyline points="22 17 13.5 8.5 8.5 13.5 2 7"/><polyline points="16 17 22 17 22 11"/></svg>',
    "info":      '<svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" class="lucide lucide-info"><circle cx="12" cy="12" r="10"/><path d="M12 16v-4"/><path d="M12 8h.01"/></svg>',
    "alert":     '<svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" class="lucide lucide-alert-triangle"><path d="m21.73 18-8-14a2 2 0 0 0-3.48 0l-8 14A2 2 0 0 0 4 21h16a2 2 0 0 0 1.73-3Z"/><path d="M12 9v4"/><path d="M12 17h.01"/></svg>',
    "check":     '<svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" class="lucide lucide-check-circle"><path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"/><polyline points="22 4 12 14.01 9 11.01"/></svg>',
    "dna":       '<svg xmlns="http://www.w3.org/2000/svg" width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" class="lucide lucide-dna"><path d="M10 16L7 19"/><path d="M14 8L17 5"/><path d="M17 10L21 6"/><path d="M12 12l-1 1"/><path d="M14 10l-2 2"/><path d="M10 14l-1 1"/><path d="M12 12l2-2"/><path d="M7 14l-4 4"/><path d="M17 19.5c-2.4-3.5-4-8.7-1-12.8"/><path d="M10 7L7 10"/><path d="M14 17l3-3"/><path d="M10 17l-3-3"/><path d="M14 7l3 3"/><path d="M7 4.5c2.4 3.5 4 8.7 1 12.8"/></svg>',
    "genre":     '<svg xmlns="http://www.w3.org/2000/svg" width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" class="lucide lucide-clapperboard"><path d="M20.2 6 3 11l-.9-2.4c-.3-.7-.1-1.5.5-1.9l16.1-4.7c.7-.2 1.5.1 1.9.7l.9 2.5c.2.3.2.6 0 .8Z"/><path d="m16 2.5 2 3.5"/><path d="m11 4 2 3.5"/><path d="m6 5.5 2 3.5"/><rect width="20" height="14.5" x="2" y="7" rx="2"/><path d="M2 12h20"/></svg>',
    "activity":  '<svg xmlns="http://www.w3.org/2000/svg" width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" class="lucide lucide-activity"><path d="M22 12h-4l-3 9L9 3l-3 9H2"/></svg>',
}


def section_header(text: str, subtitle: str = "") -> str:
    """Return styled section header HTML."""
    sub = f'<span class="section-subtitle">{subtitle}</span>' if subtitle else ""
    return f'<div class="section-header">{text}{sub}</div>'


# ─── KPI Card Helper ────────────────────────────────────────
def kpi_card(icon_name: str, label: str, value: str, delta: str = "",
             delta_color: str = "success"):
    """
    Render a premium glassmorphic KPI card using professional SVG icons.

    Args:
        icon_name:   Icon key from ICONS mapping (e.g. "ratings", "users")
        label:       KPI label (e.g. "Total Ratings")
        value:       Formatted value string (e.g. "27,753,444")
        delta:       Delta string (e.g. "Up 1,200 this year")
        delta_color: "success" → green, "danger" → red, "muted" → grey
    """
    color_map = {
        "success": "#22C55E",
        "danger":  "#EF4444",
        "muted":   "#94A3B8",
    }
    d_color = color_map.get(delta_color, "#94A3B8")
    
    # Process delta to include trending icon if missing one
    if delta and ("▲" in delta or "trending-up" in delta):
        delta = delta.replace("▲", ICONS["trending-up"])
    elif delta and ("▼" in delta or "trending-down" in delta):
        delta = delta.replace("▼", ICONS["trending-down"])
        
    delta_html = f'<div class="kpi-delta" style="color:{d_color}">{delta}</div>' if delta else ""
    icon_svg = ICONS.get(icon_name, "")

    st.markdown(f"""
    <div class="kpi-card">
        <div class="kpi-icon">{icon_svg}</div>
        <div class="kpi-label">{label}</div>
        <div class="kpi-value">{value}</div>
        {delta_html}
    </div>
    """, unsafe_allow_html=True)


# ─── Callout Helper ─────────────────────────────────────────
def callout(text: str, kind: str = "info"):
    """
    Render a styled callout box (info / warning / insight).

    Args:
        text:  Callout message (supports HTML)
        kind:  "info" → purple, "warning" → amber, "success" → green
    """
    style_map = {
        "info": {
            "bg":     "rgba(108,99,255,0.08)",
            "border": "rgba(108,99,255,0.25)",
            "icon":   ICONS["info"],
            "color":  "#A78BFA",
        },
        "warning": {
            "bg":     "rgba(245,158,11,0.08)",
            "border": "rgba(245,158,11,0.25)",
            "icon":   ICONS["alert"],
            "color":  "#F59E0B",
        },
        "success": {
            "bg":     "rgba(34,197,94,0.08)",
            "border": "rgba(34,197,94,0.25)",
            "icon":   ICONS["check"],
            "color":  "#22C55E",
        },
    }
    s = style_map.get(kind, style_map["info"])

    st.markdown(f"""
    <div class="custom-callout" style="
        background: {s['bg']};
        border: 1px solid {s['border']};
        border-radius: 12px;
        padding: 14px 18px;
        margin: 12px 0;
        display: flex;
        align-items: flex-start;
        gap: 12px;
    ">
        <span style="color: {s['color']}; display: flex; align-items: center; padding-top: 2px;">{s['icon']}</span>
        <span style="color: {s['color']}; font-family: 'Inter', sans-serif;
                      font-size: 0.88rem; line-height: 1.5;">{text}</span>
    </div>
    """, unsafe_allow_html=True)


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

    /* ── Shimmer Keyframes ───────────────────────────── */
    @keyframes shimmer {
        0%   { background-position: -200% center; }
        100% { background-position: 200% center; }
    }

    @keyframes glow-pulse {
        0%, 100% { box-shadow: 0 0 16px rgba(108,99,255,0.12); }
        50%      { box-shadow: 0 0 28px rgba(108,99,255,0.22); }
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

    /* ── Section Headers — Animated Shimmer ───────────── */
    .section-header {
        font-family: 'Fira Code', monospace;
        font-size: 1.1rem;
        font-weight: 600;
        color: var(--text);
        margin: 2rem 0 0.75rem 0;
        padding-bottom: 0.5rem;
        border-bottom: 2px solid transparent;
        background-image: linear-gradient(var(--bg), var(--bg)),
                          linear-gradient(90deg, var(--primary), var(--accent), var(--primary));
        background-origin: padding-box, border-box;
        background-clip: padding-box, border-box;
        background-size: 100% 100%, 200% 100%;
        animation: shimmer 4s ease-in-out infinite;
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

    /* ── KPI Cards (Custom HTML) ─────────────────────── */
    .kpi-card {
        background: linear-gradient(135deg, rgba(108,99,255,0.10) 0%, rgba(17,24,39,0.80) 100%);
        border: 1px solid var(--border);
        border-radius: 14px;
        padding: 20px 22px;
        backdrop-filter: blur(16px);
        -webkit-backdrop-filter: blur(16px);
        transition: all 0.3s ease-out;
        position: relative;
        overflow: hidden;
        will-change: transform;
    }
    .kpi-card::before {
        content: '';
        position: absolute;
        top: 0; left: 0; right: 0;
        height: 2px;
        background: linear-gradient(90deg, var(--primary), var(--accent), transparent);
        opacity: 0;
        transition: opacity 0.3s ease-out;
    }
    .kpi-card:hover {
        transform: translateY(-4px);
        box-shadow: 0 12px 36px rgba(108,99,255,0.20), 0 0 0 1px rgba(108,99,255,0.18);
        border-color: rgba(108,99,255,0.40);
    }
    .kpi-card:hover::before {
        opacity: 1;
    }
    .kpi-icon {
        font-size: 1.4rem;
        margin-bottom: 6px;
    }
    .kpi-label {
        color: var(--muted);
        font-family: 'Inter', sans-serif;
        font-size: 0.73rem;
        font-weight: 500;
        text-transform: uppercase;
        letter-spacing: 0.08em;
        margin-bottom: 4px;
    }
    .kpi-value {
        color: var(--text);
        font-family: 'Fira Code', monospace;
        font-weight: 700;
        font-size: 1.55rem;
        line-height: 1.2;
        margin-bottom: 4px;
    }
    .kpi-delta {
        font-family: 'Fira Code', monospace;
        font-size: 0.78rem;
        font-weight: 500;
    }

    /* ── Metric Cards (Fallback — native st.metric) ──── */
    div[data-testid="stMetric"] {
        background: linear-gradient(135deg, rgba(108,99,255,0.10) 0%, rgba(17,24,39,0.80) 100%);
        border: 1px solid var(--border);
        border-radius: 14px;
        padding: 20px 22px;
        backdrop-filter: blur(16px);
        -webkit-backdrop-filter: blur(16px);
        transition: all 0.3s ease-out;
        position: relative;
        overflow: hidden;
        will-change: transform;
    }
    div[data-testid="stMetric"]::before {
        content: '';
        position: absolute;
        top: 0; left: 0; right: 0;
        height: 2px;
        background: linear-gradient(90deg, var(--primary), var(--accent), transparent);
        opacity: 0;
        transition: opacity 0.3s ease-out;
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
        transition: all 0.3s ease-out !important;
    }
    [data-testid="stSidebar"] a[data-testid="stSidebarNavLink"]:hover {
        background: rgba(108,99,255,0.10) !important;
    }
    [data-testid="stSidebar"] a[data-testid="stSidebarNavLink"][aria-current="page"] {
        background: rgba(108,99,255,0.15) !important;
        border-left: 3px solid var(--primary) !important;
        box-shadow: inset 0 0 12px rgba(108,99,255,0.08);
    }

    /* ── Plotly Charts Container — Glow on Hover ─────── */
    [data-testid="stPlotlyChart"] {
        border-radius: 12px;
        overflow: hidden;
        border: 1px solid var(--border);
        background: var(--surface);
        padding: 8px;
        transition: box-shadow 0.3s ease-out, border-color 0.3s ease-out;
    }
    [data-testid="stPlotlyChart"]:hover {
        box-shadow: 0 0 24px rgba(108,99,255,0.18);
        border-color: rgba(108,99,255,0.35);
    }

    /* ── Status Pill ──────────────────────────────────── */
    .status-pill {
        display: inline-flex;
        align-items: center;
        gap: 6px;
        background: rgba(34,197,94,0.10);
        color: #22C55E;
        font-family: 'Fira Code', monospace;
        font-size: 0.72rem;
        padding: 3px 12px;
        border-radius: 100px;
        border: 1px solid rgba(34,197,94,0.25);
        letter-spacing: 0.02em;
    }
    .status-pill::before {
        content: '';
        width: 6px; height: 6px;
        border-radius: 50%;
        background: #22C55E;
        animation: glow-pulse 2s ease-in-out infinite;
    }

    /* ── Streamlit Callout Overrides ──────────────────── */
    [data-testid="stAlert"] {
        border-radius: 12px !important;
        backdrop-filter: blur(8px) !important;
        -webkit-backdrop-filter: blur(8px) !important;
        font-family: 'Inter', sans-serif !important;
        font-size: 0.88rem !important;
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
        transition: all 0.3s ease-out !important;
    }
    .stTabs [data-baseweb="tab"]:hover {
        color: var(--text) !important;
        background: rgba(108,99,255,0.08) !important;
    }
    .stTabs [aria-selected="true"] {
        color: var(--text) !important;
        border-bottom: 2px solid var(--primary) !important;
    }

    /* ── Hide Streamlit Branding ──────────────────────── */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header[data-testid="stHeader"] {
        background: var(--bg) !important;
        border-bottom: 1px solid var(--border) !important;
    }

    /* ── Hero Banner ─────────────────────────────────── */
    .hero-banner {
        margin-bottom: 1.5rem;
    }
    .hero-banner h1 {
        margin-bottom: 0.25rem !important;
    }
    .hero-tagline {
        font-family: 'Inter', sans-serif;
        font-size: 0.95rem;
        color: var(--muted);
        font-weight: 400;
        line-height: 1.5;
    }
    .hero-tagline strong {
        color: var(--accent);
    }

    /* ── Reduced Motion ───────────────────────────────── */
    @media (prefers-reduced-motion: reduce) {
        .section-header {
            animation: none !important;
            background-image: none !important;
            border-bottom: 2px solid var(--border) !important;
        }
        .kpi-card,
        .kpi-card::before,
        div[data-testid="stMetric"],
        div[data-testid="stMetric"]::before,
        [data-testid="stSidebar"] a,
        [data-testid="stPlotlyChart"],
        .status-pill::before {
            transition: none !important;
            animation: none !important;
        }
    }

    /* ── Responsive: Small Screens ────────────────────── */
    @media (max-width: 768px) {
        .kpi-value { font-size: 1.2rem !important; }
        .kpi-card  { padding: 14px 16px; }
        div[data-testid="stMetric"] [data-testid="stMetricValue"] {
            font-size: 1.3rem !important;
        }
    }
    </style>
    """, unsafe_allow_html=True)


def sidebar_badges(labels: list[str]):
    """Render a row of tech-stack badges in the sidebar."""
    html = " ".join(f'<span class="sidebar-badge">{lbl}</span>' for lbl in labels)
    st.markdown(html, unsafe_allow_html=True)
