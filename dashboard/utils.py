"""
utils.py — utilitários compartilhados por todas as páginas do dashboard.

Expõe:
  get_connection()   — conexão singleton com PostgreSQL (cache_resource)
  run_query(sql)     — executa SQL e retorna DataFrame (cache_data ttl=300s)
  build_sidebar()    — renderiza filtros globais e retorna dict de filtros
  kpi_card(...)      — card KPI com glassmorphism estilo dashboard financeiro
  fmt_brl(v)         — formata número como R$ 1.234,56
  fmt_pct(v)         — formata número como 12,3%
  fmt_int(v)         — formata int com separador de milhar
  check_empty(df)    — exibe mensagem amigável se DataFrame vazio
  build_where(f)     — constrói cláusula WHERE a partir dos filtros
  GRAPH_LAYOUT       — dict de layout Plotly para tema dark premium
  AXIS_STYLE         — dict de estilo de eixos minimalista
"""

import os
from datetime import datetime, timedelta

import pandas as pd
import psycopg2
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

# ── Paleta premium dark gold ───────────────────────────────────────────────
GOLD        = "#c9a84c"
GOLD_LIGHT  = "#f0c674"
GOLD_MUTED  = "rgba(201,168,76,0.15)"
BLUE        = "#4a7aff"
GREEN       = "#00c896"
RED         = "#ff5370"
AMBER       = "#fb923c"
PURPLE      = "#a78bfa"
TEAL        = "#00d4aa"

TEXT_PRI    = "#f5f5f7"
TEXT_SEC    = "#888899"
TEXT_MUT    = "#555566"
BG_BASE     = "#0e0e1a"
BG_SURFACE  = "#16162a"
BG_CARD     = "rgba(28,26,44,0.85)"
BORDER      = "rgba(255,255,255,0.06)"
BORDER_GOLD = "rgba(201,168,76,0.3)"

# Aliases mantidos para compatibilidade com páginas existentes
BRAND_COLORS = [GOLD, GOLD_LIGHT, BLUE, GREEN, RED, PURPLE, AMBER, TEAL, "#34d399", "#60a5fa"]

PLOTLY_TEMPLATE = "plotly_dark"

GRAPH_LAYOUT = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(22,22,42,0.6)",
    font=dict(family="Inter, Segoe UI, sans-serif", color="#888899", size=11),
    colorway=[GOLD, GOLD_LIGHT, BLUE, GREEN, RED, PURPLE, AMBER, TEAL, "#34d399", "#60a5fa"],
    hoverlabel=dict(
        bgcolor="#16162a",
        bordercolor=BORDER_GOLD,
        font=dict(color=TEXT_PRI, size=12),
    ),
)

AXIS_STYLE = dict(
    gridcolor="rgba(255,255,255,0.04)",
    linecolor="rgba(255,255,255,0.06)",
    tickcolor="rgba(255,255,255,0.06)",
    tickfont=dict(color=TEXT_MUT, size=10),
    zerolinecolor="rgba(255,255,255,0.06)",
)

# ── Ícones SVG inline (Feather Icons) ─────────────────────────────────────
SVG = {
    "package": '<svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="16.5" y1="9.4" x2="7.5" y2="4.21"/><path d="M21 16V8a2 2 0 0 0-1-1.73l-7-4a2 2 0 0 0-2 0l-7 4A2 2 0 0 0 3 8v8a2 2 0 0 0 1 1.73l7 4a2 2 0 0 0 2 0l7-4A2 2 0 0 0 21 16z"/><polyline points="3.27 6.96 12 12.01 20.73 6.96"/><line x1="12" y1="22.08" x2="12" y2="12"/></svg>',
    "dollar": '<svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="12" y1="1" x2="12" y2="23"/><path d="M17 5H9.5a3.5 3.5 0 0 0 0 7h5a3.5 3.5 0 0 1 0 7H6"/></svg>',
    "tag": '<svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M20.59 13.41l-7.17 7.17a2 2 0 0 1-2.83 0L2 12V2h10l8.59 8.59a2 2 0 0 1 0 2.82z"/><line x1="7" y1="7" x2="7.01" y2="7"/></svg>',
    "trending_up": '<svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="23 6 13.5 15.5 8.5 10.5 1 18"/><polyline points="17 6 23 6 23 12"/></svg>',
    "trending_down": '<svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="23 18 13.5 8.5 8.5 13.5 1 6"/><polyline points="17 18 23 18 23 12"/></svg>',
    "bar_chart": '<svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="18" y1="20" x2="18" y2="10"/><line x1="12" y1="20" x2="12" y2="4"/><line x1="6" y1="20" x2="6" y2="14"/></svg>',
    "pie_chart": '<svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21.21 15.89A10 10 0 1 1 8 2.83"/><path d="M22 12A10 10 0 0 0 12 2v10z"/></svg>',
    "truck": '<svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="1" y="3" width="15" height="13"/><polygon points="16 8 20 8 23 11 23 16 16 16 16 8"/><circle cx="5.5" cy="18.5" r="2.5"/><circle cx="18.5" cy="18.5" r="2.5"/></svg>',
    "star": '<svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polygon points="12 2 15.09 8.26 22 9.27 17 14.14 18.18 21.02 12 17.77 5.82 21.02 7 14.14 2 9.27 8.91 8.26 12 2"/></svg>',
    "percent": '<svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><line x1="19" y1="5" x2="5" y2="19"/><circle cx="6.5" cy="6.5" r="2.5"/><circle cx="17.5" cy="17.5" r="2.5"/></svg>',
    "award": '<svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="8" r="7"/><polyline points="8.21 13.89 7 23 12 20 17 23 15.79 13.88"/></svg>',
    "activity": '<svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="22 12 18 12 15 21 9 3 6 12 2 12"/></svg>',
    "calendar": '<svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="4" width="18" height="18" rx="2" ry="2"/><line x1="16" y1="2" x2="16" y2="6"/><line x1="8" y1="2" x2="8" y2="6"/><line x1="3" y1="10" x2="21" y2="10"/></svg>',
    "arrow_up": '<svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><line x1="12" y1="19" x2="12" y2="5"/><polyline points="5 12 12 5 19 12"/></svg>',
    "arrow_down": '<svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><line x1="12" y1="5" x2="12" y2="19"/><polyline points="19 12 12 19 5 12"/></svg>',
    "info": '<svg xmlns="http://www.w3.org/2000/svg" width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><line x1="12" y1="16" x2="12" y2="12"/><line x1="12" y1="8" x2="12.01" y2="8"/></svg>',
    "alert": '<svg xmlns="http://www.w3.org/2000/svg" width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M10.29 3.86L1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z"/><line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/></svg>',
    "layers": '<svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polygon points="12 2 2 7 12 12 22 7 12 2"/><polyline points="2 17 12 22 22 17"/><polyline points="2 12 12 17 22 12"/></svg>',
    "shuffle": '<svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="16 3 21 3 21 8"/><line x1="4" y1="20" x2="21" y2="3"/><polyline points="21 16 21 21 16 21"/><line x1="15" y1="15" x2="21" y2="21"/><line x1="4" y1="4" x2="9" y2="9"/></svg>',
    "grid": '<svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="3" width="7" height="7"/><rect x="14" y="3" width="7" height="7"/><rect x="14" y="14" width="7" height="7"/><rect x="3" y="14" width="7" height="7"/></svg>',
    "smartphone": '<svg xmlns="http://www.w3.org/2000/svg" width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"><rect x="5" y="2" width="14" height="20" rx="2" ry="2"/><line x1="12" y1="18" x2="12.01" y2="18"/></svg>',
    "gem": '<svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M6 3h12l4 6-10 13L2 9 6 3z"/><path d="M11 3L8 9l4 13 4-13-3-6"/><path d="M2 9h20"/></svg>',
    "refresh": '<svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="23 4 23 10 17 10"/><polyline points="1 20 1 14 7 14"/><path d="M3.51 9a9 9 0 0 1 14.85-3.36L23 10M1 14l4.64 4.36A9 9 0 0 0 20.49 15"/></svg>',
    "cpu": '<svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="4" y="4" width="16" height="16" rx="2" ry="2"/><rect x="9" y="9" width="6" height="6"/><line x1="9" y1="1" x2="9" y2="4"/><line x1="15" y1="1" x2="15" y2="4"/><line x1="9" y1="20" x2="9" y2="23"/><line x1="15" y1="20" x2="15" y2="23"/><line x1="20" y1="9" x2="23" y2="9"/><line x1="20" y1="14" x2="23" y2="14"/><line x1="1" y1="9" x2="4" y2="9"/><line x1="1" y1="14" x2="4" y2="14"/></svg>',
}


# ── Conexão PostgreSQL ─────────────────────────────────────────────────────

@st.cache_resource(show_spinner=False)
def _make_connection():
    """Cria a conexão singleton com o PostgreSQL."""
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
    """Retorna a conexão cached, reconectando se necessário."""
    conn = _make_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
        return conn
    except Exception:
        _make_connection.clear()
        return _make_connection()


# ── Execução de queries ───────────────────────────────────────────────────

@st.cache_data(ttl=300, show_spinner=False)
def run_query(sql: str) -> pd.DataFrame:
    """
    Executa um SQL e retorna DataFrame.
    Cache de 5 minutos — dados re-fetched quando SQL muda ou TTL expira.
    """
    try:
        conn = get_connection()
        return pd.read_sql_query(sql, conn)
    except Exception as exc:
        _make_connection.clear()
        try:
            conn = _make_connection()
            return pd.read_sql_query(sql, conn)
        except Exception as exc2:
            st.error(f"Erro ao carregar dados do PostgreSQL: {exc2}")
            return pd.DataFrame()


# ── Formatação ─────────────────────────────────────────────────────────────

def fmt_brl(value) -> str:
    """Formata float como R$ 1.234,56."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return "R$ —"
    v = float(value)
    formatted = f"{v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    return f"R$ {formatted}"


def fmt_pct(value) -> str:
    """Formata float como 12,3%."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return "—%"
    return f"{float(value):.1f}%".replace(".", ",")


def fmt_int(value) -> str:
    """Formata int com separador de milhar."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return "—"
    return f"{int(value):,}".replace(",", ".")


# ── CSS global (dark gold premium) ────────────────────────────────────────

def _inject_css() -> None:
    """Injeta CSS global — chamado automaticamente por build_sidebar."""
    st.markdown(
        """
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap');

        :root {
            --bg-base:         #0e0e1a;
            --bg-surface:      #16162a;
            --bg-card:         rgba(28, 26, 44, 0.85);
            --accent-gold:     #c9a84c;
            --accent-gold-lt:  #f0c674;
            --accent-gold-muted: rgba(201, 168, 76, 0.12);
            --accent-blue:     #4a7aff;
            --accent-green:    #00c896;
            --accent-red:      #ff5370;
            --text-primary:    #f5f5f7;
            --text-secondary:  #888899;
            --text-muted:      #555566;
            --border-subtle:   rgba(255,255,255,0.06);
            --border-gold:     rgba(201,168,76,0.3);
            --shadow-card:     0 8px 40px rgba(0,0,0,0.6);
            --shadow-hover:    0 16px 48px rgba(0,0,0,0.7);
            --radius-card:     16px;
            --radius-inner:    10px;
        }

        html, body, [class*="css"], .stApp {
            font-family: 'Inter', 'Segoe UI', sans-serif !important;
            background-color: var(--bg-base) !important;
            color: var(--text-primary) !important;
        }

        /* ── Layout ──────────────────────────────────────── */
        .block-container { padding-top: 1.5rem !important; }
        a { text-decoration: none !important; }

        /* ── Scrollbar ───────────────────────────────────── */
        ::-webkit-scrollbar { width: 5px; height: 5px; }
        ::-webkit-scrollbar-track { background: var(--bg-base); }
        ::-webkit-scrollbar-thumb {
            background: var(--accent-gold);
            border-radius: 3px;
            opacity: 0.6;
        }

        /* ── Sidebar ─────────────────────────────────────── */
        section[data-testid="stSidebar"] > div:first-child {
            background: var(--bg-surface) !important;
            border-right: 1px solid var(--border-subtle);
        }

        /* ── KPI Flip Cards ──────────────────────────────── */
        .flip-wrapper {
            perspective: 1000px;
            height: 148px;
            cursor: pointer;
        }
        .flip-inner {
            position: relative;
            width: 100%;
            height: 100%;
            transition: transform 0.55s cubic-bezier(0.4, 0, 0.2, 1);
            transform-style: preserve-3d;
        }
        .flip-wrapper:hover .flip-inner {
            transform: rotateY(180deg);
        }
        .flip-front,
        .flip-back {
            position: absolute;
            width: 100%;
            height: 100%;
            backface-visibility: hidden;
            -webkit-backface-visibility: hidden;
            border-radius: 14px;
            padding: 18px 20px;
            box-sizing: border-box;
            border: 1px solid var(--border-subtle);
            border-top: 2px solid var(--accent-gold);
            background: var(--bg-card);
            backdrop-filter: blur(20px);
            -webkit-backdrop-filter: blur(20px);
            box-shadow: var(--shadow-card);
            overflow: hidden;
        }
        .flip-front::before {
            content: '';
            position: absolute;
            top: 0; left: 0; right: 0;
            height: 1px;
            background: linear-gradient(90deg, transparent, var(--accent-gold), transparent);
        }
        .flip-back {
            transform: rotateY(180deg);
            background: linear-gradient(135deg, rgba(42,36,20,0.97) 0%, rgba(28,24,12,0.95) 100%);
            border-top-color: var(--accent-gold-lt);
            display: flex;
            flex-direction: column;
            justify-content: space-between;
        }
        .card-top {
            display: flex;
            justify-content: space-between;
            align-items: flex-start;
            margin-bottom: 8px;
        }
        .card-label {
            font-size: 0.62rem;
            font-weight: 600;
            letter-spacing: 0.1em;
            text-transform: uppercase;
            color: var(--text-secondary);
        }
        .card-icon-box {
            width: 30px; height: 30px;
            background: var(--accent-gold-muted);
            border-radius: 8px;
            display: flex;
            align-items: center;
            justify-content: center;
        }
        .card-icon-box svg {
            width: 14px; height: 14px;
            stroke: var(--accent-gold);
            fill: none;
            stroke-width: 1.8;
            stroke-linecap: round;
            stroke-linejoin: round;
        }
        .card-icon-box.blue  { background: rgba(74,122,255,0.12); }
        .card-icon-box.blue  svg { stroke: var(--accent-blue); }
        .card-icon-box.green { background: rgba(0,200,150,0.12); }
        .card-icon-box.green svg { stroke: var(--accent-green); }
        .card-icon-box.red   { background: rgba(255,83,112,0.12); }
        .card-icon-box.red   svg { stroke: var(--accent-red); }
        .card-icon-box.purple{ background: rgba(167,139,250,0.12); }
        .card-icon-box.purple svg { stroke: #a78bfa; }
        .card-value {
            font-size: 1.6rem;
            font-weight: 800;
            color: var(--text-primary);
            line-height: 1.1;
            letter-spacing: -0.02em;
            margin-bottom: 8px;
            font-variant-numeric: tabular-nums;
        }
        .card-bottom {
            display: flex;
            align-items: center;
            gap: 6px;
            flex-wrap: wrap;
        }
        .card-delta {
            font-size: 0.72rem;
            font-weight: 700;
        }
        .card-delta.positive { color: var(--accent-green); }
        .card-delta.negative { color: var(--accent-red); }
        .card-delta.neutral  { color: var(--text-secondary); }
        .card-sub {
            font-size: 0.68rem;
            color: var(--text-muted);
        }
        /* ── Flip Back ───────────────────────────────────── */
        .back-title {
            font-size: 0.6rem;
            font-weight: 600;
            letter-spacing: 0.1em;
            text-transform: uppercase;
            color: var(--accent-gold);
            margin-bottom: 2px;
        }
        .back-main {
            font-size: 1.2rem;
            font-weight: 800;
            color: var(--accent-gold-lt);
            letter-spacing: -0.02em;
        }
        .back-divider {
            height: 1px;
            background: linear-gradient(90deg, var(--accent-gold), transparent);
            margin: 6px 0;
        }
        .back-insight {
            font-size: 0.7rem;
            color: var(--text-secondary);
            line-height: 1.45;
            flex: 1;
        }
        .back-comparisons {
            display: flex;
            gap: 12px;
            margin-top: 6px;
            flex-wrap: wrap;
        }
        .back-comp-item {
            display: flex;
            flex-direction: column;
            gap: 1px;
        }
        .comp-label {
            font-size: 0.58rem;
            text-transform: uppercase;
            letter-spacing: 0.08em;
            color: var(--text-muted);
        }
        .comp-value {
            font-size: 0.8rem;
            font-weight: 700;
            color: var(--accent-gold-lt);
        }

        /* ── Section Header ──────────────────────────────── */
        .section-header {
            display: flex;
            align-items: center;
            gap: 14px;
            margin-bottom: 6px;
            padding-top: 4px;
        }
        .section-icon {
            width: 38px;
            height: 38px;
            display: flex;
            align-items: center;
            justify-content: center;
            background: var(--accent-gold-muted);
            border-radius: 10px;
            color: var(--accent-gold);
            flex-shrink: 0;
        }
        .section-icon.blue   { background: rgba(74,122,255,0.12); color: var(--accent-blue); }
        .section-icon.green  { background: rgba(0,200,150,0.12);  color: var(--accent-green); }
        .section-icon.red    { background: rgba(255,83,112,0.12); color: var(--accent-red); }
        .section-icon.purple { background: rgba(167,139,250,0.12);color: #a78bfa; }
        .section-text {}
        .section-title {
            font-size: 1.0rem;
            font-weight: 700;
            color: var(--text-primary);
            margin: 0;
            line-height: 1.2;
        }
        .section-subtitle {
            font-size: 0.78rem;
            color: var(--text-secondary);
            margin: 2px 0 0 0;
        }
        .section-divider {
            height: 1px;
            background: linear-gradient(90deg, var(--accent-gold) 0%, var(--border-subtle) 60%, transparent 100%);
            margin-bottom: 20px;
            margin-top: 10px;
        }

        /* ── Insight Box ─────────────────────────────────── */
        .insight-box {
            display: flex;
            align-items: flex-start;
            gap: 10px;
            background: var(--accent-gold-muted);
            border-left: 3px solid var(--accent-gold);
            border-radius: 0 var(--radius-inner) var(--radius-inner) 0;
            padding: 12px 16px;
            margin-top: 10px;
            box-shadow: 0 2px 16px rgba(0,0,0,0.3);
        }
        .insight-box .ins-icon {
            color: var(--accent-gold);
            flex-shrink: 0;
            margin-top: 1px;
        }
        .insight-box .ins-text {
            font-size: 0.83rem;
            color: var(--text-primary);
            line-height: 1.5;
        }
        .insight-box.blue {
            background: rgba(74,122,255,0.08);
            border-color: var(--accent-blue);
        }
        .insight-box.blue .ins-icon { color: var(--accent-blue); }
        .insight-box.green {
            background: rgba(0,200,150,0.08);
            border-color: var(--accent-green);
        }
        .insight-box.green .ins-icon { color: var(--accent-green); }
        .insight-box.red {
            background: rgba(255,83,112,0.08);
            border-color: var(--accent-red);
        }
        .insight-box.red .ins-icon { color: var(--accent-red); }

        /* ── Tabs ────────────────────────────────────────── */
        .stTabs [data-baseweb="tab-list"] {
            gap: 4px;
            background: transparent;
            border-bottom: 1px solid var(--border-subtle);
        }
        .stTabs [data-baseweb="tab"] {
            background: transparent;
            color: var(--text-secondary) !important;
            border-radius: 6px 6px 0 0;
            font-size: 0.82rem;
            font-weight: 500;
            padding: 8px 16px;
        }
        .stTabs [aria-selected="true"] {
            background: var(--accent-gold-muted) !important;
            color: var(--accent-gold) !important;
            border-bottom: 2px solid var(--accent-gold) !important;
        }

        /* ── Table hover ─────────────────────────────────── */
        tr:hover td {
            background: rgba(201,168,76,0.04) !important;
            transition: background 0.15s ease;
        }

        /* ── Alert banner ────────────────────────────────── */
        .alert-banner {
            display: flex;
            align-items: flex-start;
            gap: 10px;
            background: rgba(251,146,60,0.08);
            border-left: 3px solid #fb923c;
            border-radius: 0 8px 8px 0;
            padding: 10px 14px;
            margin-bottom: 14px;
            font-size: 0.83rem;
            color: var(--text-primary);
        }
        .alert-banner .alr-icon { color: #fb923c; flex-shrink: 0; margin-top: 1px; }
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
    delta_type: str = "neutral",      # "positive" | "negative" | "neutral"
    sub: str = "",
    icon_color: str = "",             # "" = gold, "blue", "green", "red", "purple"
    back_insight: str = "",           # texto assertivo no verso
    back_comps: list = None,          # [{"label": str, "value": str}, ...]
    # Legacy compat (ignored)
    warm: bool = False,
    delta_color: str = "",
    bg: str = "",
    accent: str = "",
) -> None:
    """Card KPI com flip 3D — frente: valor, verso: insight + comparações."""
    svg_icon = SVG.get(icon_key, SVG["bar_chart"])
    icon_cls = f" {icon_color}" if icon_color else ""

    # Delta front
    if delta:
        arr = "↑ " if delta_type == "positive" else ("↓ " if delta_type == "negative" else "")
        delta_html = f'<span class="card-delta {delta_type}">{arr}{delta}</span>'
    else:
        delta_html = ""

    sub_html = f'<span class="card-sub">{sub}</span>' if sub else ""

    # Comparações verso
    comps_html = ""
    if back_comps:
        items = "".join(
            f'<div class="back-comp-item">'
            f'<span class="comp-label">{c["label"]}</span>'
            f'<span class="comp-value">{c["value"]}</span>'
            f'</div>'
            for c in back_comps
        )
        comps_html = f'<div class="back-comparisons">{items}</div>'

    back_content = (
        f'<div class="back-title">{label}</div>'
        f'<div class="back-main">{value}</div>'
        f'<div class="back-divider"></div>'
        f'<div class="back-insight">{back_insight}</div>'
        f'{comps_html}'
    ) if back_insight else (
        f'<div class="back-title">{label}</div>'
        f'<div class="back-main">{value}</div>'
        f'<div class="back-divider"></div>'
        f'{comps_html}'
    )

    st.markdown(
        f"""
        <div class="flip-wrapper">
          <div class="flip-inner">
            <div class="flip-front">
              <div class="card-top">
                <span class="card-label">{label}</span>
                <div class="card-icon-box{icon_cls}">{svg_icon}</div>
              </div>
              <div class="card-value">{value}</div>
              <div class="card-bottom">{delta_html}{sub_html}</div>
            </div>
            <div class="flip-back">
              {back_content}
            </div>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def section_header(title: str, subtitle: str = "", color: str = GOLD, icon_key: str = "bar_chart") -> None:
    """Cabeçalho de seção com ícone SVG e divisor dourado."""
    # Mapeia cor para classe CSS
    color_map = {GOLD: "", BLUE: "blue", GREEN: "green", RED: "red", PURPLE: "purple", AMBER: ""}
    cls = color_map.get(color, "")
    svg_icon = SVG.get(icon_key, SVG["bar_chart"])
    sub_html = f'<p class="section-subtitle">{subtitle}</p>' if subtitle else ""

    st.markdown(
        f"""
        <div class="section-header">
          <div class="section-icon{' ' + cls if cls else ''}">{svg_icon}</div>
          <div class="section-text">
            <h3 class="section-title">{title}</h3>
            {sub_html}
          </div>
        </div>
        <div class="section-divider"></div>
        """,
        unsafe_allow_html=True,
    )


def insight_box(text: str, color: str = GOLD) -> None:
    """Caixa de insight com borda dourada e ícone SVG."""
    color_map = {GOLD: "", BLUE: "blue", GREEN: "green", RED: "red", AMBER: ""}
    cls = color_map.get(color, "")
    cls_str = f" {cls}" if cls else ""
    st.markdown(
        f'<div class="insight-box{cls_str}">'
        f'<span class="ins-icon">{SVG["info"]}</span>'
        f'<span class="ins-text">{text}</span>'
        f'</div>',
        unsafe_allow_html=True,
    )


def check_empty(df: pd.DataFrame, msg: str = "Nenhum dado disponível para os filtros selecionados.") -> bool:
    """Retorna True e exibe alerta amigável se o DataFrame estiver vazio."""
    if df.empty:
        st.info(f"{msg}")
        return True
    return False


# ── Filtros da sidebar ─────────────────────────────────────────────────────

def build_sidebar() -> dict:
    """
    Renderiza a sidebar com os filtros globais e retorna um dict com os valores.
    Deve ser chamada no topo de cada página.
    Também injeta o CSS global.
    """
    _inject_css()

    with st.sidebar:
        st.markdown(
            f"""
            <div style="text-align:center;padding:20px 0 18px">
              <div style="
                  display:inline-flex;align-items:center;justify-content:center;
                  width:48px;height:48px;
                  background:var(--accent-gold-muted);
                  border:1px solid var(--border-gold);
                  border-radius:14px;
                  color:var(--accent-gold);
                  margin-bottom:10px;
              ">{SVG["smartphone"]}</div>
              <div style="font-weight:700;font-size:1.0rem;color:var(--text-primary);line-height:1.2">
                Smartphones BR
              </div>
              <div style="font-size:0.7rem;color:var(--text-muted);margin-top:4px">
                Amazon via RapidAPI
              </div>
            </div>
            <div style="height:1px;background:var(--border-subtle);margin-bottom:18px"></div>
            """,
            unsafe_allow_html=True,
        )
        st.markdown(
            f'<div style="font-size:0.72rem;font-weight:600;text-transform:uppercase;'
            f'letter-spacing:0.08em;color:var(--text-muted);margin-bottom:10px">'
            f'Filtros</div>',
            unsafe_allow_html=True,
        )

        # ── Condição ──────────────────────────────────────────────────────
        condition_labels = {"Todos": "Todos", "new": "Novo", "used": "Usado"}
        condition = st.selectbox(
            "Condição",
            options=list(condition_labels.keys()),
            format_func=lambda x: condition_labels[x],
        )

        # ── Faixa de preço ────────────────────────────────────────────────
        price_range = st.slider(
            "Faixa de Preço",
            min_value=0, max_value=15000,
            value=(0, 15000), step=250,
            format="R$ %d",
        )

        # ── Marcas ────────────────────────────────────────────────────────
        try:
            brands_df = run_query(
                "SELECT DISTINCT brand FROM marts.fct_products "
                "WHERE brand IS NOT NULL ORDER BY brand"
            )
            all_brands = brands_df["brand"].tolist() if not brands_df.empty else []
        except Exception:
            all_brands = []

        selected_brands = st.multiselect(
            "Marca",
            options=all_brands,
            placeholder="Todas as marcas",
        )

        # ── Período ───────────────────────────────────────────────────────
        period_days = st.selectbox(
            "Período",
            options=[7, 15, 30],
            format_func=lambda x: f"Últimos {x} dias",
        )

        cutoff_date = (datetime.now() - timedelta(days=period_days)).strftime("%Y-%m-%d")

        st.markdown(
            f'<div style="height:1px;background:var(--border-subtle);margin:18px 0 10px"></div>'
            f'<div style="font-size:0.68rem;color:var(--text-muted);text-align:center">'
            f'Cache 5 min &nbsp;·&nbsp; Schema: marts</div>',
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


# ── Construção dinâmica de WHERE ──────────────────────────────────────────

def build_where(filters: dict, table_alias: str = "") -> str:
    """
    Constrói uma cláusula WHERE a partir dos filtros da sidebar.
    Todos os valores vêm de controles de UI (não de texto livre do usuário).
    """
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
