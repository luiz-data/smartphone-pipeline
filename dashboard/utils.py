"""
utils.py — utilitários compartilhados por todas as páginas do dashboard.

Expõe:
  get_connection()   — conexão singleton com PostgreSQL (cache_resource)
  run_query(sql)     — executa SQL e retorna DataFrame (cache_data ttl=300s)
  build_sidebar()    — renderiza filtros globais e retorna dict de filtros
  kpi_card(...)      — renderiza um card KPI estilizado
  fmt_brl(v)         — formata número como R$ 1.234,56
  fmt_pct(v)         — formata número como 12,3%
  check_empty(df)    — exibe mensagem amigável se DataFrame vazio
  build_where(f)     — constrói cláusula WHERE a partir dos filtros
"""

import os
from datetime import datetime, timedelta

import pandas as pd
import psycopg2
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

# ── Paleta de cores ────────────────────────────────────────────────────────
BLUE    = "#2563EB"
GREEN   = "#16A34A"
RED     = "#DC2626"
AMBER   = "#D97706"
SLATE   = "#64748B"
BORDER  = "#E2E8F0"
BG_CARD = "#FFFFFF"
TEXT_PRI = "#1E293B"
TEXT_SEC = "#64748B"

PLOTLY_TEMPLATE = "plotly_white"
PRIMARY_SEQ     = "Blues"   # sequência azul para gráficos de barra
BRAND_COLORS    = [
    "#2563EB", "#16A34A", "#DC2626", "#D97706", "#7C3AED",
    "#0891B2", "#DB2777", "#EA580C", "#65A30D", "#475569",
]


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
    Cache de 5 minutos (ttl=300s): dados são re-fetched a cada 5 min
    ou quando a SQL muda (ex.: filtros diferentes geram SQL diferente).
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
            st.error(f"❌ Erro ao carregar dados do PostgreSQL: {exc2}")
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


# ── KPI Card ───────────────────────────────────────────────────────────────

def kpi_card(
    icon: str,
    label: str,
    value: str,
    delta: str = "",
    delta_color: str = GREEN,
    bg: str = BG_CARD,
) -> None:
    """Renderiza um card KPI estilizado com ícone, label, valor e delta."""
    delta_html = (
        f'<div style="font-size:.78rem;color:{delta_color};'
        f'margin-top:5px;font-weight:500">{delta}</div>'
        if delta else ""
    )
    st.markdown(
        f"""
        <div style="
            background:{bg};
            border-radius:12px;
            border:1px solid {BORDER};
            padding:22px 12px;
            text-align:center;
            box-shadow:0 1px 4px rgba(0,0,0,.06);
            min-height:148px;
            display:flex;
            flex-direction:column;
            justify-content:center;
            align-items:center;
        ">
            <div style="font-size:1.6rem;line-height:1">{icon}</div>
            <div style="
                font-size:.72rem;color:{TEXT_SEC};font-weight:600;
                text-transform:uppercase;letter-spacing:.07em;margin-top:6px
            ">{label}</div>
            <div style="
                font-size:1.65rem;font-weight:800;color:{TEXT_PRI};
                margin-top:4px;line-height:1.1;word-break:break-all
            ">{value}</div>
            {delta_html}
        </div>
        """,
        unsafe_allow_html=True,
    )


def _spacer(h: int = 16) -> None:
    st.markdown(f'<div style="height:{h}px"></div>', unsafe_allow_html=True)


def section_header(title: str, subtitle: str = "") -> None:
    """Cabeçalho de seção com divisor visual."""
    st.markdown(f"### {title}")
    if subtitle:
        st.markdown(f'<p style="color:{TEXT_SEC};margin-top:-10px">{subtitle}</p>',
                    unsafe_allow_html=True)
    st.markdown('<hr style="border:none;border-top:1px solid #E2E8F0;margin:8px 0 18px 0">',
                unsafe_allow_html=True)


def insight_box(text: str, color: str = BLUE) -> None:
    """Caixa de insight automático abaixo de cada gráfico."""
    st.markdown(
        f'<div style="'
        f'background:{color}11;border-left:4px solid {color};'
        f'padding:10px 14px;border-radius:0 8px 8px 0;'
        f'font-size:.85rem;color:{TEXT_PRI};margin-top:6px'
        f'">💡 {text}</div>',
        unsafe_allow_html=True,
    )


def check_empty(df: pd.DataFrame, msg: str = "Nenhum dado disponível para os filtros selecionados.") -> bool:
    """Retorna True e exibe alerta amigável se o DataFrame estiver vazio."""
    if df.empty:
        st.info(f"ℹ️ {msg}")
        return True
    return False


# ── Filtros da sidebar ─────────────────────────────────────────────────────

def build_sidebar() -> dict:
    """
    Renderiza a sidebar com os filtros globais e retorna um dict com os valores.
    Deve ser chamada no topo de cada página.

    Retorna:
      {
        "condition":   "Todos" | "new" | "used",
        "min_price":   int,
        "max_price":   int,
        "brands":      list[str],   # vazio = todas as marcas
        "period_days": int,
        "cutoff_date": str,         # ISO date para uso em SQL
      }
    """
    with st.sidebar:
        st.markdown(
            f'<div style="text-align:center;padding:8px 0 16px">'
            f'<span style="font-size:2rem">📱</span><br>'
            f'<span style="font-weight:700;font-size:1.1rem;color:{TEXT_PRI}">'
            f'Smartphones BR</span><br>'
            f'<span style="font-size:.75rem;color:{TEXT_SEC}">Amazon via RapidAPI</span>'
            f'</div>',
            unsafe_allow_html=True,
        )
        st.markdown("---")
        st.markdown("#### 🔧 Filtros Globais")

        # ── Condição ──────────────────────────────────────────────────────
        condition_labels = {"Todos": "Todos", "new": "🆕 Novo", "used": "🔄 Usado"}
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

        st.markdown("---")
        st.markdown(
            f'<div style="font-size:.72rem;color:{TEXT_SEC};text-align:center">'
            f'Cache: 5 min &nbsp;|&nbsp; Schema: marts</div>',
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

    Args:
      filters:     dict retornado por build_sidebar()
      table_alias: prefixo de tabela (ex.: "p." para "p.price")
    """
    prefix = f"{table_alias}." if table_alias else ""
    clauses = [f"{prefix}price > 0"]

    # Faixa de preço
    clauses.append(f"{prefix}price BETWEEN {filters['min_price']} AND {filters['max_price']}")

    # Condição
    if filters["condition"] != "Todos":
        cond = filters["condition"].replace("'", "''")
        clauses.append(f"{prefix}condition = '{cond}'")

    # Marcas (IN clause)
    if filters["brands"]:
        escaped = ", ".join("'" + b.replace("'", "''") + "'" for b in filters["brands"])
        clauses.append(f"{prefix}brand IN ({escaped})")

    # Período
    clauses.append(f"{prefix}collected_at >= '{filters['cutoff_date']}'")

    return "WHERE " + "\n  AND ".join(clauses)
