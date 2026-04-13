"""
utils.py — utilitários compartilhados por todas as páginas do dashboard.
"""

import os
from datetime import datetime, timedelta

import pandas as pd
import psycopg2
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

# ── Tokens de cor ──────────────────────────────────────────────────────────
GOLD        = "#c9a06a"
GOLD_LIGHT  = "#e8c48a"
GOLD_DARK   = "#a07848"
GREEN       = "#4caf7d"
RED         = "#e05c5c"
BLUE        = "#5b8dee"
AMBER       = "#fb923c"
PURPLE      = "#a78bfa"
TEAL        = "#00c896"

TEXT_PRI      = "#1a1b21"
TEXT_SEC      = "#4a4b5a"
TEXT_MUT      = "#9a9aaa"
BG_BASE       = "#f5f6fa"
BG_SURFACE    = "#ffffff"
BG_CARD       = "#ffffff"
BG_CARD_HOVER = "#f8f9fc"
BORDER        = "rgba(0,0,0,0.06)"
BORDER_GOLD   = "rgba(201,160,106,0.35)"

BRAND_COLORS = [GOLD, GOLD_LIGHT, BLUE, GREEN, RED, PURPLE, AMBER, TEAL, "#34d399", "#60a5fa"]

PLOTLY_TEMPLATE = "plotly_white"

GRAPH_LAYOUT = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(248,249,252,0.6)",
    font=dict(family="Inter, sans-serif", color="#4a4b5a", size=10),
    colorway=["#c9a06a", "#e8c48a", "#2ecc71", "#3498db", "#e74c3c", "#9b59b6", "#f39c12"],
    hoverlabel=dict(
        bgcolor="#ffffff",
        bordercolor="rgba(201,160,106,0.4)",
        font=dict(color="#1a1b21", size=11),
        namelength=0,
    ),
)

AXIS_STYLE = dict(
    gridcolor="rgba(0,0,0,0.05)",
    linecolor="rgba(0,0,0,0.08)",
    tickcolor="rgba(0,0,0,0)",
    tickfont=dict(color="#9a9aaa", size=9),
    zerolinecolor="rgba(0,0,0,0.06)",
)

# ── Ícones SVG (Feather) ───────────────────────────────────────────────────
SVG = {
    "smartphone": '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"><rect x="5" y="2" width="14" height="20" rx="2"/><line x1="12" y1="18" x2="12.01" y2="18"/></svg>',
    "home": '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"><path d="M3 9l9-7 9 7v11a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2z"/><polyline points="9 22 9 12 15 12 15 22"/></svg>',
    "award": '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="8" r="7"/><polyline points="8.21 13.89 7 23 12 20 17 23 15.79 13.88"/></svg>',
    "activity": '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"><polyline points="22 12 18 12 15 21 9 3 6 12 2 12"/></svg>',
    "settings": '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="3"/><path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 0 1-2.83 2.83l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 0 1-4 0v-.09A1.65 1.65 0 0 0 9 19.4a1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 0 1-2.83-2.83l.06-.06A1.65 1.65 0 0 0 4.68 15a1.65 1.65 0 0 0-1.51-1H3a2 2 0 0 1 0-4h.09A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 0 1 2.83-2.83l.06.06A1.65 1.65 0 0 0 9 4.68a1.65 1.65 0 0 0 1-1.51V3a2 2 0 0 1 4 0v.09a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 0 1 2.83 2.83l-.06.06A1.65 1.65 0 0 0 19.4 9a1.65 1.65 0 0 0 1.51 1H21a2 2 0 0 1 0 4h-.09a1.65 1.65 0 0 0-1.51 1z"/></svg>',
    "package": '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><line x1="16.5" y1="9.4" x2="7.5" y2="4.21"/><path d="M21 16V8a2 2 0 0 0-1-1.73l-7-4a2 2 0 0 0-2 0l-7 4A2 2 0 0 0 3 8v8a2 2 0 0 0 1 1.73l7 4a2 2 0 0 0 2 0l7-4A2 2 0 0 0 21 16z"/><polyline points="3.27 6.96 12 12.01 20.73 6.96"/><line x1="12" y1="22.08" x2="12" y2="12"/></svg>',
    "dollar": '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><line x1="12" y1="1" x2="12" y2="23"/><path d="M17 5H9.5a3.5 3.5 0 0 0 0 7h5a3.5 3.5 0 0 1 0 7H6"/></svg>',
    "tag": '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M20.59 13.41l-7.17 7.17a2 2 0 0 1-2.83 0L2 12V2h10l8.59 8.59a2 2 0 0 1 0 2.82z"/><line x1="7" y1="7" x2="7.01" y2="7"/></svg>',
    "trending_up": '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><polyline points="23 6 13.5 15.5 8.5 10.5 1 18"/><polyline points="17 6 23 6 23 12"/></svg>',
    "trending_down": '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><polyline points="23 18 13.5 8.5 8.5 13.5 1 6"/><polyline points="17 18 23 18 23 12"/></svg>',
    "bar_chart": '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><line x1="18" y1="20" x2="18" y2="10"/><line x1="12" y1="20" x2="12" y2="4"/><line x1="6" y1="20" x2="6" y2="14"/></svg>',
    "pie_chart": '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M21.21 15.89A10 10 0 1 1 8 2.83"/><path d="M22 12A10 10 0 0 0 12 2v10z"/></svg>',
    "truck": '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><rect x="1" y="3" width="15" height="13"/><polygon points="16 8 20 8 23 11 23 16 16 16 16 8"/><circle cx="5.5" cy="18.5" r="2.5"/><circle cx="18.5" cy="18.5" r="2.5"/></svg>',
    "star": '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><polygon points="12 2 15.09 8.26 22 9.27 17 14.14 18.18 21.02 12 17.77 5.82 21.02 7 14.14 2 9.27 8.91 8.26 12 2"/></svg>',
    "percent": '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><line x1="19" y1="5" x2="5" y2="19"/><circle cx="6.5" cy="6.5" r="2.5"/><circle cx="17.5" cy="17.5" r="2.5"/></svg>',
    "layers": '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><polygon points="12 2 2 7 12 12 22 7 12 2"/><polyline points="2 17 12 22 22 17"/><polyline points="2 12 12 17 22 12"/></svg>',
    "calendar": '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="4" width="18" height="18" rx="2"/><line x1="16" y1="2" x2="16" y2="6"/><line x1="8" y1="2" x2="8" y2="6"/><line x1="3" y1="10" x2="21" y2="10"/></svg>',
    "refresh": '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><polyline points="23 4 23 10 17 10"/><polyline points="1 20 1 14 7 14"/><path d="M3.51 9a9 9 0 0 1 14.85-3.36L23 10M1 14l4.64 4.36A9 9 0 0 0 20.49 15"/></svg>',
    "gem": '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M6 3h12l4 6-10 13L2 9 6 3z"/><path d="M11 3L8 9l4 13 4-13-3-6"/><path d="M2 9h20"/></svg>',
    "shuffle": '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><polyline points="16 3 21 3 21 8"/><line x1="4" y1="20" x2="21" y2="3"/><polyline points="21 16 21 21 16 21"/><line x1="15" y1="15" x2="21" y2="21"/><line x1="4" y1="4" x2="9" y2="9"/></svg>',
    "alert": '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M10.29 3.86L1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z"/><line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/></svg>',
}


# ── Conexão PostgreSQL ─────────────────────────────────────────────────────

@st.cache_resource(show_spinner=False)
def _make_connection():
    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=int(os.environ.get("POSTGRES_PORT", 5432)),
        dbname=os.environ.get("POSTGRES_DB", "smartphones"),
        user=os.environ.get("POSTGRES_USER", "pipeline_user"),
        password=os.environ.get("POSTGRES_PASSWORD", ""),
        options="-c search_path=marts,staging,raw,public",
        connect_timeout=10,
    )


def get_connection():
    conn = _make_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
        return conn
    except Exception:
        _make_connection.clear()
        return _make_connection()


@st.cache_data(ttl=300, show_spinner=False)
def run_query(sql: str) -> pd.DataFrame:
    try:
        conn = get_connection()
        return pd.read_sql_query(sql, conn)
    except Exception:
        _make_connection.clear()
        try:
            conn = _make_connection()
            return pd.read_sql_query(sql, conn)
        except Exception as exc2:
            st.error(f"Erro ao carregar dados: {exc2}")
            return pd.DataFrame()


# ── Formatação ─────────────────────────────────────────────────────────────

def fmt_brl(value) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return "R$ —"
    v = float(value)
    return "R$ " + f"{v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def fmt_pct(value) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return "—%"
    return f"{float(value):.1f}%".replace(".", ",")


def fmt_int(value) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return "—"
    return f"{int(value):,}".replace(",", ".")


# ── CSS global ─────────────────────────────────────────────────────────────

def _inject_css() -> None:
    st.markdown(
        """
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800;900&display=swap');

        :root {
            --bg-base:           #f5f6fa;
            --bg-surface:        #ffffff;
            --bg-card:           #ffffff;
            --bg-card-hover:     #f8f9fc;
            --accent-gold:       #c9a06a;
            --accent-gold-light: #e8c48a;
            --accent-gold-dark:  #a07848;
            --accent-green:      #4caf7d;
            --accent-red:        #e05c5c;
            --accent-blue:       #5b8dee;
            --text-primary:      #1a1b21;
            --text-secondary:    #4a4b5a;
            --text-muted:        #9a9aaa;
            --border-subtle:     rgba(0,0,0,0.06);
            --border-gold:       rgba(201,160,106,0.35);
            --shadow-sm:         0 2px 8px rgba(0,0,0,0.06);
            --shadow-md:         0 4px 20px rgba(0,0,0,0.08);
            --shadow-lg:         0 8px 40px rgba(0,0,0,0.10);
            --radius-sm:         8px;
            --radius-md:         14px;
            --radius-lg:         20px;
            --transition:        all 0.25s cubic-bezier(0.4,0,0.2,1);
        }

        *, *::before, *::after { box-sizing: border-box; }
        * { font-family: 'Inter', sans-serif !important; }

        html, body, .stApp, [class*="css"] {
            background-color: var(--bg-base) !important;
            color: var(--text-primary) !important;
        }

        .block-container { padding-top: 1.1rem !important; }
        a { text-decoration: none !important; }
        p { margin: 0; }

        /* ── Scrollbar ───────────────────────────────────── */
        ::-webkit-scrollbar { width: 4px; height: 4px; }
        ::-webkit-scrollbar-track { background: transparent; }
        ::-webkit-scrollbar-thumb {
            background: rgba(201,160,106,0.35);
            border-radius: 2px;
        }

        /* ── Sidebar ─────────────────────────────────────── */
        section[data-testid="stSidebar"] > div:first-child {
            background: var(--bg-surface) !important;
            border-right: 1px solid var(--border-subtle);
        }
        [data-testid="stSidebarNav"] {
            padding: 0 10px 6px;
        }
        [data-testid="stSidebarNav"] a {
            display: flex;
            align-items: center;
            gap: 8px;
            padding: 7px 10px;
            border-radius: var(--radius-sm);
            font-size: 0.76rem;
            font-weight: 500;
            color: var(--text-muted) !important;
            transition: var(--transition);
        }
        [data-testid="stSidebarNav"] a:hover {
            background: rgba(255,255,255,0.04);
            color: var(--text-secondary) !important;
        }
        [data-testid="stSidebarNav"] a[aria-current="page"] {
            background: rgba(201,160,106,0.1);
            color: var(--accent-gold) !important;
        }

        /* ── Sidebar content ─────────────────────────────── */
        .sb-logo-wrap {
            display: flex; justify-content: center;
            padding: 14px 0 6px;
        }
        .sb-logo {
            width: 40px; height: 40px;
            background: linear-gradient(135deg, var(--accent-gold-dark), var(--accent-gold));
            border-radius: var(--radius-sm);
            display: flex; align-items: center; justify-content: center;
            box-shadow: 0 4px 12px rgba(201,160,106,0.3);
            color: #fff;
        }
        .sb-logo svg {
            width: 20px; height: 20px;
            stroke: #fff; fill: none;
            stroke-width: 1.6;
            stroke-linecap: round; stroke-linejoin: round;
        }
        .sb-divider {
            height: 1px;
            background: var(--border-subtle);
            margin: 10px 0 6px;
        }
        .sb-filter-label {
            font-size: 0.62rem;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 0.09em;
            color: var(--text-muted);
            margin: 10px 0 4px;
            display: block;
        }
        .sb-footer {
            font-size: 0.6rem;
            color: var(--text-muted);
            text-align: center;
            padding: 10px 0;
            border-top: 1px solid var(--border-subtle);
            margin-top: 10px;
        }

        /* ── Hero bar ────────────────────────────────────── */
        .hero-bar {
            display: flex;
            justify-content: space-between;
            align-items: center;
            background: #ffffff;
            border: 1px solid rgba(0,0,0,0.06);
            border-left: 3px solid #c9a06a;
            border-radius: var(--radius-md);
            padding: 14px 22px;
            margin-bottom: 22px;
            box-shadow: 0 2px 12px rgba(0,0,0,0.06);
        }
        .hero-left {
            display: flex; align-items: center; gap: 14px;
        }
        .hero-icon {
            width: 44px; height: 44px;
            background: linear-gradient(135deg, rgba(201,160,106,0.18), rgba(201,160,106,0.04));
            border: 1px solid var(--border-gold);
            border-radius: var(--radius-sm);
            display: flex; align-items: center; justify-content: center;
            flex-shrink: 0;
        }
        .hero-icon svg {
            width: 22px; height: 22px;
            stroke: var(--accent-gold); fill: none; stroke-width: 1.6;
            stroke-linecap: round; stroke-linejoin: round;
        }
        .hero-title {
            font-size: 1.05rem; font-weight: 700;
            color: var(--text-primary); margin: 0; line-height: 1.2;
        }
        .hero-sub {
            font-size: 0.7rem; color: var(--text-muted); margin: 2px 0 0 0;
        }
        .hero-right {
            display: flex; flex-direction: column;
            align-items: flex-end; gap: 4px;
        }
        .hero-badge {
            font-size: 0.66rem; color: var(--accent-gold);
            background: rgba(201,160,106,0.07);
            border: 1px solid var(--border-gold);
            border-radius: 20px; padding: 3px 13px; font-weight: 500;
        }
        .hero-meta { font-size: 0.62rem; color: var(--text-muted); }

        /* ── KPI Flip Cards ──────────────────────────────── */
        .kpi-flip {
            perspective: 1200px;
            height: 172px;
        }
        .kpi-flip-inner {
            position: relative;
            width: 100%; height: 100%;
            transform-style: preserve-3d;
            transition: transform 0.5s cubic-bezier(0.4, 0, 0.2, 1);
        }
        .kpi-flip:hover .kpi-flip-inner {
            transform: rotateY(180deg);
        }
        .kpi-front, .kpi-back {
            position: absolute; inset: 0;
            backface-visibility: hidden;
            -webkit-backface-visibility: hidden;
            border-radius: var(--radius-md);
            padding: 18px 20px;
            box-sizing: border-box;
            background: #ffffff;
            border: 1px solid rgba(0,0,0,0.06);
            box-shadow: 0 2px 12px rgba(0,0,0,0.06);
            overflow: hidden;
        }
        .kpi-front::after {
            content: '';
            position: absolute;
            top: 0; left: 0; right: 0; height: 2px;
            background: linear-gradient(90deg,
                transparent, rgba(201,160,106,0.4), transparent);
            border-radius: var(--radius-md) var(--radius-md) 0 0;
        }
        .kpi-back {
            transform: rotateY(180deg);
            background: linear-gradient(135deg, #fdf8f0 0%, #faf4e8 100%);
            border-color: var(--border-gold);
            display: flex; flex-direction: column;
            justify-content: space-between;
        }
        .kpi-header {
            display: flex;
            justify-content: space-between;
            align-items: flex-start;
            margin-bottom: 9px;
        }
        .kpi-label-text {
            font-size: 0.6rem; font-weight: 600;
            letter-spacing: 0.1em; text-transform: uppercase;
            color: var(--text-muted);
        }
        .kpi-icon-wrap {
            width: 27px; height: 27px;
            background: rgba(201,160,106,0.1);
            border-radius: 7px;
            display: flex; align-items: center; justify-content: center;
            flex-shrink: 0;
        }
        .kpi-icon-wrap svg {
            width: 13px; height: 13px;
            stroke: var(--accent-gold); fill: none;
            stroke-width: 1.8; stroke-linecap: round; stroke-linejoin: round;
        }
        .kpi-icon-wrap.blue  { background: rgba(91,141,238,0.1); }
        .kpi-icon-wrap.blue  svg { stroke: var(--accent-blue); }
        .kpi-icon-wrap.green { background: rgba(76,175,125,0.1); }
        .kpi-icon-wrap.green svg { stroke: var(--accent-green); }
        .kpi-icon-wrap.red   { background: rgba(224,92,92,0.1); }
        .kpi-icon-wrap.red   svg { stroke: var(--accent-red); }
        .kpi-icon-wrap.purple{ background: rgba(167,139,250,0.1); }
        .kpi-icon-wrap.purple svg { stroke: #a78bfa; }
        .kpi-value-text {
            font-size: 1.5rem; font-weight: 800;
            color: var(--text-primary); line-height: 1;
            letter-spacing: -0.025em;
            margin-bottom: 7px;
            font-variant-numeric: tabular-nums;
        }
        .kpi-footer-row {
            display: flex; align-items: center; gap: 7px; flex-wrap: wrap;
        }
        .kpi-delta-text {
            font-size: 0.7rem; font-weight: 700;
            display: flex; align-items: center; gap: 2px;
        }
        .kpi-delta-text.pos, .kpi-delta-text.positive { color: var(--accent-green); }
        .kpi-delta-text.neg, .kpi-delta-text.negative { color: var(--accent-red); }
        .kpi-delta-text.neu, .kpi-delta-text.neutral  { color: var(--text-muted); }
        .kpi-sub-text { font-size: 0.63rem; color: var(--text-muted); }
        /* Verso */
        .back-label {
            font-size: 0.57rem; font-weight: 700;
            letter-spacing: 0.12em; text-transform: uppercase;
            color: var(--accent-gold);
        }
        .back-value {
            font-size: 1.05rem; font-weight: 800;
            color: var(--accent-gold-light);
            letter-spacing: -0.02em; margin: 2px 0;
        }
        .back-divider {
            height: 1px;
            background: linear-gradient(90deg, var(--accent-gold-dark), transparent);
            margin: 4px 0;
        }
        .back-text {
            font-size: 0.65rem; color: var(--text-secondary);
            line-height: 1.4; flex: 1;
        }
        .back-comps {
            display: flex; gap: 12px; padding-top: 4px;
            border-top: 1px solid rgba(0,0,0,0.06);
            flex-wrap: wrap;
        }
        .back-comp { display: flex; flex-direction: column; gap: 1px; }
        .comp-k {
            font-size: 0.54rem; text-transform: uppercase;
            letter-spacing: 0.08em; color: var(--text-muted);
        }
        .comp-v { font-size: 0.73rem; font-weight: 700; color: var(--accent-gold-light); }

        /* ── Section Header ──────────────────────────────── */
        .sec-head {
            display: flex; align-items: center;
            gap: 12px; margin-bottom: 8px; padding: 2px 0;
        }
        .sec-icon {
            width: 34px; height: 34px;
            background: rgba(201,160,106,0.1);
            border: 1px solid var(--border-gold);
            border-radius: var(--radius-sm);
            display: flex; align-items: center; justify-content: center;
            flex-shrink: 0;
        }
        .sec-icon svg {
            width: 15px; height: 15px;
            stroke: var(--accent-gold); fill: none;
            stroke-width: 1.7; stroke-linecap: round; stroke-linejoin: round;
        }
        .sec-icon.blue  { background: rgba(91,141,238,0.1); border-color: rgba(91,141,238,0.25); }
        .sec-icon.blue  svg { stroke: var(--accent-blue); }
        .sec-icon.green { background: rgba(76,175,125,0.1); border-color: rgba(76,175,125,0.25); }
        .sec-icon.green svg { stroke: var(--accent-green); }
        .sec-icon.red   { background: rgba(224,92,92,0.1); border-color: rgba(224,92,92,0.25); }
        .sec-icon.red   svg { stroke: var(--accent-red); }
        .sec-title {
            font-size: 0.9rem; font-weight: 700;
            color: var(--text-primary); line-height: 1.2;
        }
        .sec-sub { font-size: 0.7rem; color: var(--text-muted); margin-top: 1px; }
        .sec-line {
            height: 1px;
            background: linear-gradient(90deg,
                #c9a06a 0%, rgba(201,160,106,0.1) 50%,
                transparent 100%);
            margin-bottom: 16px;
        }

        /* ── Insight Box ─────────────────────────────────── */
        .insight {
            display: flex; align-items: flex-start; gap: 10px;
            background: rgba(201,160,106,0.06);
            border: 1px solid rgba(201,160,106,0.2);
            border-left: 2px solid var(--accent-gold-dark);
            border-radius: 0 var(--radius-sm) var(--radius-sm) 0;
            padding: 10px 14px; margin: 10px 0;
            font-size: 0.77rem; color: #4a4b5a; line-height: 1.5;
        }
        .insight-dot {
            width: 6px; height: 6px; border-radius: 50%;
            background: var(--accent-gold);
            flex-shrink: 0; margin-top: 5px;
        }
        .insight.green {
            background: rgba(76,175,125,0.04);
            border-color: rgba(76,175,125,0.12);
            border-left-color: var(--accent-green);
        }
        .insight.green .insight-dot { background: var(--accent-green); }
        .insight.blue {
            background: rgba(91,141,238,0.04);
            border-color: rgba(91,141,238,0.12);
            border-left-color: var(--accent-blue);
        }
        .insight.blue .insight-dot { background: var(--accent-blue); }
        .insight.red {
            background: rgba(224,92,92,0.04);
            border-color: rgba(224,92,92,0.12);
            border-left-color: var(--accent-red);
        }
        .insight.red .insight-dot { background: var(--accent-red); }

        /* ── Alert banner ────────────────────────────────── */
        .alert-banner {
            display: flex; align-items: flex-start; gap: 10px;
            background: rgba(251,146,60,0.06);
            border-left: 2px solid #fb923c;
            border-radius: 0 var(--radius-sm) var(--radius-sm) 0;
            padding: 9px 13px; margin-bottom: 12px;
            font-size: 0.76rem; color: var(--text-secondary);
        }
        .alert-banner svg { stroke: #fb923c; flex-shrink: 0; margin-top: 2px; }

        /* ── Tabs ────────────────────────────────────────── */
        .stTabs [data-baseweb="tab-list"] {
            gap: 2px; background: transparent;
            border-bottom: 1px solid var(--border-subtle);
        }
        .stTabs [data-baseweb="tab"] {
            background: transparent;
            color: var(--text-muted) !important;
            border-radius: 6px 6px 0 0;
            font-size: 0.78rem; font-weight: 500;
            padding: 7px 14px;
        }
        .stTabs [aria-selected="true"] {
            background: rgba(201,160,106,0.08) !important;
            color: var(--accent-gold) !important;
            border-bottom: 2px solid var(--accent-gold) !important;
        }

        /* ── Data Table ──────────────────────────────────── */
        .data-table {
            width: 100%;
            border-collapse: collapse;
            table-layout: fixed;
        }
        .data-table th {
            overflow: hidden;
            text-overflow: ellipsis;
            white-space: nowrap;
            padding: 10px 12px;
            text-align: left;
            background: rgba(201,160,106,0.08);
            color: var(--accent-gold);
            font-size: 0.65rem;
            font-weight: 700;
            text-transform: uppercase;
            letter-spacing: 0.08em;
            border-bottom: 1px solid rgba(201,160,106,0.2);
        }
        .data-table td {
            overflow: hidden;
            text-overflow: ellipsis;
            white-space: nowrap;
            padding: 10px 12px;
            font-size: 0.82rem;
            color: var(--text-primary);
            border-bottom: 1px solid rgba(0,0,0,0.04);
        }
        .data-table tr:nth-child(even) td {
            background: rgba(0,0,0,0.015);
        }
        .data-table tr:hover td {
            background: rgba(201,160,106,0.04);
            transition: background 0.15s ease;
        }

        /* ── SIDEBAR BASE ─────────────────────────── */
        section[data-testid="stSidebar"] {
            background: #ffffff;
            border-right: 1px solid rgba(0,0,0,0.06);
            padding-top: 0;
        }

        section[data-testid="stSidebar"] > div {
            padding-top: 16px;
        }

        /* ── NAVEGAÇÃO — container ───────────────── */
        section[data-testid="stSidebar"] nav {
            padding: 0 8px;
        }

        /* ── NAVEGAÇÃO — links ───────────────────── */
        section[data-testid="stSidebar"] nav a {
            display: flex;
            align-items: center;
            gap: 10px;
            padding: 10px 14px;
            border-radius: 10px;
            margin-bottom: 4px;
            font-size: 0.875rem;
            font-weight: 500;
            color: #4a4b5a;
            text-decoration: none !important;
            transition: background 0.2s ease,
                        color 0.2s ease,
                        border 0.2s ease;
            border-left: 3px solid transparent;
        }

        /* ── HOVER ───────────────────────────────── */
        section[data-testid="stSidebar"] nav a:hover {
            background: #f2ede7;
            color: #c9a06a;
            border-left: 3px solid rgba(201,160,106,0.4);
        }

        /* ── ITEM ATIVO ──────────────────────────── */
        section[data-testid="stSidebar"] nav a[aria-current="page"] {
            background: #e8dfd5;
            color: #c9a06a;
            font-weight: 700;
            border-left: 3px solid #c9a06a;
        }

        section[data-testid="stSidebar"] nav a[aria-current="page"] p {
            color: #c9a06a !important;
            font-weight: 700 !important;
        }

        /* ── TEXTO DOS LINKS ─────────────────────── */
        section[data-testid="stSidebar"] nav a p {
            font-size: 0.875rem !important;
            margin: 0 !important;
            color: #4a4b5a;
            transition: color 0.2s ease;
        }

        section[data-testid="stSidebar"] nav a:hover p {
            color: #c9a06a !important;
        }

        /* ── SEPARADOR nav / filtros ─────────────── */
        section[data-testid="stSidebar"] [data-testid="stSidebarUserContent"] {
            padding-top: 8px;
            border-top: 1px solid #e5e5e5;
            margin-top: 8px;
        }

        /* ── LOGO / TÍTULO DO SIDEBAR ────────────── */
        section[data-testid="stSidebar"] [data-testid="stSidebarUserContent"] > div:first-child {
            padding-bottom: 12px;
            border-bottom: 1px solid #e5e5e5;
            margin-bottom: 12px;
        }

        /* ── LABELS DOS FILTROS ──────────────────── */
        section[data-testid="stSidebar"] .stSelectbox label,
        section[data-testid="stSidebar"] .stSlider label,
        section[data-testid="stSidebar"] .stMultiSelect label {
            font-size: 0.65rem !important;
            font-weight: 700 !important;
            text-transform: uppercase !important;
            letter-spacing: 0.08em !important;
            color: #888888 !important;
            margin-top: 12px !important;
        }

        /* ── SELECTBOX ───────────────────────────── */
        section[data-testid="stSidebar"] .stSelectbox > div > div {
            border-radius: 8px !important;
            border-color: rgba(0,0,0,0.08) !important;
            font-size: 0.85rem !important;
            background: #fafafa !important;
        }

        section[data-testid="stSidebar"] .stSelectbox > div > div:focus-within {
            border-color: #c9a06a !important;
            box-shadow: 0 0 0 2px rgba(201,160,106,0.2) !important;
        }

        /* ── SLIDER ──────────────────────────────── */
        section[data-testid="stSidebar"] .stSlider [data-testid="stThumbValue"] {
            color: #c9a06a !important;
            font-weight: 600 !important;
            font-size: 0.75rem !important;
        }

        /* ── ESPAÇAMENTO entre filtros ───────────── */
        section[data-testid="stSidebar"] .stSelectbox,
        section[data-testid="stSidebar"] .stSlider,
        section[data-testid="stSidebar"] .stMultiSelect {
            margin-bottom: 8px !important;
        }

        /* ── RODAPÉ DO SIDEBAR ───────────────────── */
        section[data-testid="stSidebar"] [data-testid="stSidebarUserContent"] > div:last-child {
            border-top: 1px solid #e5e5e5;
            padding-top: 12px;
            margin-top: 16px;
            font-size: 0.65rem;
            color: #9a9aaa;
        }

        /* ── SCROLLBAR DO SIDEBAR ────────────────── */
        section[data-testid="stSidebar"]::-webkit-scrollbar {
            width: 4px;
        }

        section[data-testid="stSidebar"]::-webkit-scrollbar-track {
            background: #ffffff;
        }

        section[data-testid="stSidebar"]::-webkit-scrollbar-thumb {
            background: rgba(201,160,106,0.3);
            border-radius: 10px;
        }

        section[data-testid="stSidebar"]::-webkit-scrollbar-thumb:hover {
            background: #c9a06a;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


# ── KPI Flip Card ─────────────────────────────────────────────────────────

def kpi_card(
    icon_key: str,
    label: str,
    value: str,
    delta: str = "",
    delta_type: str = "neutral",
    sub: str = "",
    icon_color: str = "",
    back_insight: str = "",
    back_comps: list = None,
    # legacy compat
    warm: bool = False,
    delta_color: str = "",
    bg: str = "",
    accent: str = "",
) -> None:
    """Card KPI com flip 3D — frente: valor; verso: insight + comparações."""
    svg = SVG.get(icon_key, SVG["bar_chart"])
    icls = f" {icon_color}" if icon_color else ""

    # Normalise delta_type → CSS class
    _dt = {"positive": "pos", "negative": "neg", "neutral": "neu",
           "pos": "pos", "neg": "neg", "neu": "neu"}
    dcls = _dt.get(delta_type, "neu")

    delta_html = ""
    if delta:
        arr = "↑ " if dcls == "pos" else ("↓ " if dcls == "neg" else "")
        delta_html = f'<span class="kpi-delta-text {dcls}">{arr}{delta}</span>'

    sub_html = f'<span class="kpi-sub-text">{sub}</span>' if sub else ""

    comps_html = ""
    if back_comps:
        items = "".join(
            f'<div class="back-comp">'
            f'<span class="comp-k">{c["label"]}</span>'
            f'<span class="comp-v">{c["value"]}</span>'
            f'</div>'
            for c in back_comps
        )
        comps_html = f'<div class="back-comps">{items}</div>'

    back_html = (
        f'<div class="back-label">{label}</div>'
        f'<div class="back-value">{value}</div>'
        f'<div class="back-divider"></div>'
        f'<div class="back-text">{back_insight}</div>'
        f'{comps_html}'
    )

    st.markdown(
        f"""
        <div class="kpi-flip">
          <div class="kpi-flip-inner">
            <div class="kpi-front">
              <div class="kpi-header">
                <span class="kpi-label-text">{label}</span>
                <div class="kpi-icon-wrap{icls}">{svg}</div>
              </div>
              <div class="kpi-value-text">{value}</div>
              <div class="kpi-footer-row">{delta_html}{sub_html}</div>
            </div>
            <div class="kpi-back">{back_html}</div>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


# ── Section Header ─────────────────────────────────────────────────────────

def section_header(title: str, subtitle: str = "", color: str = GOLD, icon_key: str = "bar_chart") -> None:
    _cmap = {GOLD: "", BLUE: "blue", GREEN: "green", RED: "red", AMBER: "", PURPLE: "purple"}
    cls = _cmap.get(color, "")
    svg = SVG.get(icon_key, SVG["bar_chart"])
    sub_html = f'<div class="sec-sub">{subtitle}</div>' if subtitle else ""
    cls_str = f" {cls}" if cls else ""

    st.markdown(
        f"""
        <div class="sec-head">
          <div class="sec-icon{cls_str}">{svg}</div>
          <div class="sec-text">
            <div class="sec-title">{title}</div>
            {sub_html}
          </div>
        </div>
        <div class="sec-line"></div>
        """,
        unsafe_allow_html=True,
    )


# ── Insight Box ────────────────────────────────────────────────────────────

def insight_box(text: str, color: str = GOLD) -> None:
    _cmap = {GOLD: "", BLUE: "blue", GREEN: "green", RED: "red", AMBER: "", TEAL: "green"}
    cls = _cmap.get(color, "")
    cls_str = f" {cls}" if cls else ""
    st.markdown(
        f'<div class="insight{cls_str}">'
        f'<div class="insight-dot"></div>'
        f'<span>{text}</span>'
        f'</div>',
        unsafe_allow_html=True,
    )


# ── Check empty ────────────────────────────────────────────────────────────

def check_empty(df: pd.DataFrame, msg: str = "Nenhum dado disponível para os filtros selecionados.") -> bool:
    if df.empty:
        st.info(msg)
        return True
    return False


# ── Sidebar ────────────────────────────────────────────────────────────────

def build_sidebar() -> dict:
    _inject_css()

    with st.sidebar:
        # Logo
        st.markdown(
            f"""
            <div class="sb-logo-wrap">
              <div class="sb-logo">{SVG["smartphone"]}</div>
            </div>
            <div class="sb-divider"></div>
            """,
            unsafe_allow_html=True,
        )

        # Condição
        st.markdown('<span class="sb-filter-label">Condição</span>', unsafe_allow_html=True)
        condition_labels = {"Todos": "Todos", "new": "Novo", "used": "Usado"}
        condition = st.selectbox(
            "Condição", options=list(condition_labels.keys()),
            format_func=lambda x: condition_labels[x],
            label_visibility="collapsed",
        )

        # Faixa de preço
        st.markdown('<span class="sb-filter-label">Faixa de Preço</span>', unsafe_allow_html=True)
        price_range = st.slider(
            "Faixa de Preço", min_value=0, max_value=15000,
            value=(0, 15000), step=250, format="R$ %d",
            label_visibility="collapsed",
        )

        # Marcas
        st.markdown('<span class="sb-filter-label">Marca</span>', unsafe_allow_html=True)
        try:
            brands_df = run_query(
                "SELECT DISTINCT brand FROM marts.fct_products "
                "WHERE brand IS NOT NULL ORDER BY brand"
            )
            all_brands = brands_df["brand"].tolist() if not brands_df.empty else []
        except Exception:
            all_brands = []

        selected_brands = st.multiselect(
            "Marca", options=all_brands, placeholder="Todas",
            label_visibility="collapsed",
        )

        # Período
        st.markdown('<span class="sb-filter-label">Período</span>', unsafe_allow_html=True)
        period_days = st.selectbox(
            "Período", options=[7, 15, 30],
            format_func=lambda x: f"Últimos {x} dias",
            label_visibility="collapsed",
        )

        cutoff_date = (datetime.now() - timedelta(days=period_days)).strftime("%Y-%m-%d")

        st.markdown(
            '<div class="sb-footer">Cache 5 min &nbsp;·&nbsp; marts</div>',
            unsafe_allow_html=True,
        )

    return {
        "condition":   condition,
        "min_price":   price_range[0],
        "max_price":   price_range[1],
        "brands":      selected_brands,
        "period_days": period_days,
        "cutoff_date": cutoff_date,
    }


# ── WHERE clause ───────────────────────────────────────────────────────────

def build_where(filters: dict, table_alias: str = "") -> str:
    prefix = f"{table_alias}." if table_alias else ""
    clauses = [f"{prefix}price > 0"]
    clauses.append(f"{prefix}price BETWEEN {filters['min_price']} AND {filters['max_price']}")
    if filters["condition"] != "Todos":
        cond = filters["condition"].replace("'", "''")
        clauses.append(f"{prefix}condition = '{cond}'")
    if filters["brands"]:
        escaped = ", ".join("'" + b.replace("'", "''") + "'" for b in filters["brands"])
        clauses.append(f"{prefix}brand IN ({escaped})")
    clauses.append(f"{prefix}collected_at >= '{filters['cutoff_date']}'")
    return "WHERE " + "\n  AND ".join(clauses)
