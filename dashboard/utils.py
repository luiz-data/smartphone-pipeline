"""
utils.py — utilitários compartilhados por todas as páginas do dashboard.

Expõe:
  get_connection()   — conexão singleton com PostgreSQL (cache_resource)
  run_query(sql)     — executa SQL e retorna DataFrame (cache_data ttl=300s)
  build_sidebar()    — renderiza filtros globais e retorna dict de filtros
  kpi_card(...)      — card KPI com efeito flip (frente: valor, verso: delta/contexto)
  fmt_brl(v)         — formata número como R$ 1.234,56
  fmt_pct(v)         — formata número como 12,3%
  check_empty(df)    — exibe mensagem amigável se DataFrame vazio
  build_where(f)     — constrói cláusula WHERE a partir dos filtros
  GRAPH_LAYOUT       — dict de layout Plotly para tema dark consistente
  AXIS_STYLE         — dict de estilo de eixos para tema dark
"""

import os
from datetime import datetime, timedelta

import pandas as pd
import psycopg2
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

# ── Paleta dark mode ───────────────────────────────────────────────────────
BLUE    = "#4f8ef7"
GREEN   = "#00d4aa"
RED     = "#ff4757"
AMBER   = "#ff6b35"
PURPLE  = "#7c5cbf"
SLATE   = "#8892a4"
BORDER  = "#2d3748"
BG_CARD = "#1a1d2e"
BG_PAGE = "#0f1117"
TEXT_PRI = "#e8eaf6"
TEXT_SEC = "#8892a4"

PLOTLY_TEMPLATE = "plotly_dark"
PRIMARY_SEQ     = "plasma"
BRAND_COLORS    = [
    "#4f8ef7", "#00d4aa", "#7c5cbf", "#ff6b35", "#ff4757",
    "#00cec9", "#fd79a8", "#fdcb6e", "#a29bfe", "#55efc4",
]

# Layout padrão Plotly — aplicar em todos os fig.update_layout()
GRAPH_LAYOUT = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(26,29,46,0.9)",
    font=dict(color="#e8eaf6"),
)
AXIS_STYLE = dict(gridcolor="#2d3748", linecolor="#2d3748")


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


# ── CSS global (dark mode) ─────────────────────────────────────────────────

def _inject_css() -> None:
    """Injeta CSS global de dark mode — chamado automaticamente por build_sidebar."""
    st.markdown(
        """
        <style>
        /* ── Layout ──────────────────────────────────────── */
        .block-container { padding-top: 1.5rem !important; }
        a { text-decoration: none !important; }

        /* ── Scrollbar ───────────────────────────────────── */
        ::-webkit-scrollbar { width: 6px; height: 6px; }
        ::-webkit-scrollbar-track { background: #1a1d2e; }
        ::-webkit-scrollbar-thumb { background: #4f8ef7; border-radius: 3px; }

        /* ── Sidebar ─────────────────────────────────────── */
        section[data-testid="stSidebar"] > div:first-child {
            background: #1a1d2e !important;
            border-right: 1px solid #2d3748;
        }

        /* ── Flip card ───────────────────────────────────── */
        .flip-card {
            perspective: 1000px;
            height: 148px;
            cursor: default;
        }
        .flip-card-inner {
            position: relative;
            width: 100%;
            height: 100%;
            transition: transform 0.6s cubic-bezier(0.4, 0, 0.2, 1);
            transform-style: preserve-3d;
        }
        .flip-card:hover .flip-card-inner {
            transform: rotateY(180deg);
        }
        .flip-card-front,
        .flip-card-back {
            position: absolute;
            width: 100%;
            height: 100%;
            backface-visibility: hidden;
            -webkit-backface-visibility: hidden;
            border-radius: 12px;
            display: flex;
            flex-direction: column;
            justify-content: center;
            align-items: center;
            padding: 16px 12px;
            box-sizing: border-box;
            box-shadow: 0 4px 20px rgba(0,0,0,0.35);
            background: #1a1d2e;
            text-align: center;
        }
        .flip-card-back {
            transform: rotateY(180deg);
        }

        /* ── Table hover ─────────────────────────────────── */
        tr:hover td {
            background: rgba(79,142,247,0.08) !important;
            transition: background 0.15s ease;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


# ── KPI Card com flip ──────────────────────────────────────────────────────

def kpi_card(
    icon: str,
    label: str,
    value: str,
    delta: str = "",
    delta_color: str = GREEN,
    bg: str = BG_CARD,
    accent: str = BLUE,
) -> None:
    """
    Card KPI com efeito flip 3D.
    Frente: ícone + label + valor principal.
    Verso:  delta/contexto (se fornecido) ou repetição do ícone.
    """
    if delta:
        back_content = (
            f'<div style="font-size:.72rem;color:{TEXT_SEC};text-transform:uppercase;'
            f'letter-spacing:.06em;margin-bottom:10px">{label}</div>'
            f'<div style="font-size:1.5rem;font-weight:700;color:{delta_color};'
            f'line-height:1.1">{delta}</div>'
        )
    else:
        back_content = (
            f'<div style="font-size:2.2rem;line-height:1;margin-bottom:8px">{icon}</div>'
            f'<div style="font-size:.72rem;color:{TEXT_SEC};text-transform:uppercase;'
            f'letter-spacing:.06em">{label}</div>'
        )

    st.markdown(
        f"""
        <div class="flip-card">
          <div class="flip-card-inner">
            <div class="flip-card-front" style="border-top:3px solid {accent}">
              <div style="font-size:1.7rem;line-height:1">{icon}</div>
              <div style="font-size:.7rem;color:{TEXT_SEC};font-weight:600;
                  text-transform:uppercase;letter-spacing:.07em;margin-top:7px">
                {label}
              </div>
              <div style="font-size:1.5rem;font-weight:800;color:{TEXT_PRI};
                  margin-top:6px;line-height:1.1;word-break:break-all">
                {value}
              </div>
            </div>
            <div class="flip-card-back" style="border-top:3px solid {accent}">
              {back_content}
            </div>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _spacer(h: int = 16) -> None:
    st.markdown(f'<div style="height:{h}px"></div>', unsafe_allow_html=True)


def section_header(title: str, subtitle: str = "", color: str = BLUE) -> None:
    """Cabeçalho de seção com banner gradiente e borda colorida."""
    sub_html = (
        f'<div style="font-size:.82rem;color:{TEXT_SEC};margin-top:4px">{subtitle}</div>'
        if subtitle else ""
    )
    st.markdown(
        f"""
        <div style="
            background:linear-gradient(135deg,{color}25 0%,{color}08 100%);
            border-left:4px solid {color};
            border-radius:0 8px 8px 0;
            padding:14px 20px;
            margin-bottom:18px;
        ">
            <div style="font-size:1.05rem;font-weight:700;color:{TEXT_PRI}">{title}</div>
            {sub_html}
        </div>
        """,
        unsafe_allow_html=True,
    )


def insight_box(text: str, color: str = BLUE) -> None:
    """Caixa de insight automático com borda colorida e fundo semi-transparente."""
    st.markdown(
        f'<div style="'
        f'background:{color}18;border-left:4px solid {color};'
        f'padding:12px 16px;border-radius:0 8px 8px 0;'
        f'font-size:.85rem;color:{TEXT_PRI};margin-top:8px;'
        f'box-shadow:0 2px 12px rgba(0,0,0,0.25);'
        f'font-style:italic'
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
    Também injeta o CSS global de dark mode.
    """
    _inject_css()

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
