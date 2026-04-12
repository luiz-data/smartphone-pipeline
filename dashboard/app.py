"""
app.py — Ponto de entrada do dashboard Smartphones BR.

Configura a página, verifica a conexão com o banco de dados e exibe
a landing page com resumo do mercado e links de navegação.
"""

import streamlit as st
from utils import (
    get_connection,
    run_query,
    build_sidebar,
    kpi_card,
    fmt_brl,
    fmt_pct,
    fmt_int,
    check_empty,
    build_where,
    TEXT_PRI,
    TEXT_SEC,
    BLUE,
    GREEN,
    RED,
    AMBER,
    PURPLE,
    BORDER,
    BG_CARD,
    BRAND_COLORS,
    PLOTLY_TEMPLATE,
    GRAPH_LAYOUT,
)

st.set_page_config(
    page_title="Smartphones BR — Dashboard",
    page_icon="📱",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Sidebar (injeta CSS global) ────────────────────────────────────────────
filters = build_sidebar()

# ── Verificação de conexão ─────────────────────────────────────────────────
try:
    get_connection()
    db_ok = True
except Exception as exc:
    db_ok = False
    st.error(f"❌ Sem conexão com o PostgreSQL: {exc}")

# ── Hero ───────────────────────────────────────────────────────────────────
st.markdown(
    f"""
    <div style="
        text-align:center;
        padding:36px 24px 28px;
        background:linear-gradient(135deg,#0d1b4b 0%,#1a2a6c 45%,#1e3a8a 100%);
        border-radius:16px;
        margin-bottom:28px;
        border:1px solid #2d3a6b;
        box-shadow:0 8px 32px rgba(0,0,0,0.4);
    ">
        <div style="font-size:3.5rem;line-height:1;margin-bottom:10px">📱</div>
        <h1 style="color:#e8eaf6;font-size:2rem;font-weight:800;margin:0">
            Smartphones BR
        </h1>
        <p style="color:#93c5fd;font-size:1rem;margin-top:8px">
            Monitoramento de preços na Amazon Brasil via RapidAPI
        </p>
        <div style="
            display:inline-block;
            background:rgba(79,142,247,0.15);
            border:1px solid rgba(79,142,247,0.3);
            border-radius:20px;
            padding:4px 16px;
            font-size:.78rem;
            color:#93c5fd;
            margin-top:10px;
        ">
            Collector → Redis → PostgreSQL → dbt → Streamlit
        </div>
    </div>
    """,
    unsafe_allow_html=True,
)

# ── KPIs rápidos ───────────────────────────────────────────────────────────
if db_ok:
    where = build_where(filters)
    sql_kpi = f"""
        SELECT
            COUNT(*)                          AS total_produtos,
            ROUND(AVG(price)::numeric, 2)     AS preco_medio,
            MIN(price)                        AS preco_min,
            COUNT(DISTINCT brand)             AS total_marcas,
            ROUND(
                100.0 * SUM(CASE WHEN free_shipping THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0),
                1
            )                                 AS pct_frete_gratis
        FROM marts.fct_products
        {where}
    """
    df_kpi = run_query(sql_kpi)

    if not df_kpi.empty and df_kpi["total_produtos"].iloc[0]:
        row = df_kpi.iloc[0]
        c1, c2, c3, c4, c5 = st.columns(5)
        with c1:
            kpi_card("📦", "Produtos", fmt_int(row["total_produtos"]), accent=BLUE)
        with c2:
            kpi_card("💰", "Preço Médio", fmt_brl(row["preco_medio"]), accent=GREEN)
        with c3:
            kpi_card("🏷️", "Menor Preço", fmt_brl(row["preco_min"]), accent=PURPLE)
        with c4:
            kpi_card("🏷️", "Marcas", fmt_int(row["total_marcas"]), accent=AMBER)
        with c5:
            kpi_card("🚚", "Frete Grátis", fmt_pct(row["pct_frete_gratis"]), accent=GREEN)
    else:
        st.info("ℹ️ Nenhum dado disponível para os filtros selecionados.")

    st.markdown("<br>", unsafe_allow_html=True)

# ── Cards de navegação ─────────────────────────────────────────────────────
st.markdown(
    f'<div style="font-size:1.05rem;font-weight:700;color:{TEXT_PRI};margin-bottom:6px">'
    f'Páginas do Dashboard</div>'
    f'<div style="font-size:.85rem;color:{TEXT_SEC};margin-bottom:18px">'
    f'Selecione uma análise no menu lateral ou clique abaixo</div>',
    unsafe_allow_html=True,
)

nav_cols = st.columns(3)

nav_cards = [
    {
        "icon": "📊",
        "title": "Visão Geral do Mercado",
        "desc": "Preços, distribuição por condição, frete grátis e histograma de faixas de preço.",
        "questions": "P1 · P2 · P6 · P8 · P9",
        "color": BLUE,
    },
    {
        "icon": "🏆",
        "title": "Marcas & Competitividade",
        "desc": "Ranking de marcas por volume, dispersão desconto × avaliações e score de competitividade.",
        "questions": "P3 · P4 · P10",
        "color": PURPLE,
    },
    {
        "icon": "📈",
        "title": "Evolução de Preços",
        "desc": "Série temporal de preços com banda de confiança e variação por produto.",
        "questions": "P5 · P7",
        "color": GREEN,
    },
]

for col, card in zip(nav_cols, nav_cards):
    with col:
        st.markdown(
            f"""
            <div style="
                background:{BG_CARD};
                border:1px solid {BORDER};
                border-top:3px solid {card['color']};
                border-radius:12px;
                padding:24px 16px;
                min-height:200px;
                box-shadow:0 4px 20px rgba(0,0,0,0.3);
                transition:transform 0.2s ease,box-shadow 0.2s ease;
            ">
                <div style="font-size:2.2rem;line-height:1">{card['icon']}</div>
                <div style="font-size:1rem;font-weight:700;color:{TEXT_PRI};margin-top:10px">
                    {card['title']}
                </div>
                <div style="font-size:.82rem;color:{TEXT_SEC};margin-top:6px;line-height:1.45">
                    {card['desc']}
                </div>
                <div style="
                    display:inline-block;
                    background:{card['color']}22;
                    color:{card['color']};
                    font-size:.72rem;font-weight:700;
                    border-radius:20px;
                    padding:3px 12px;
                    margin-top:12px;
                    border:1px solid {card['color']}44;
                ">{card['questions']}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

# ── Rodapé ─────────────────────────────────────────────────────────────────
st.markdown("<br>", unsafe_allow_html=True)
st.markdown(
    f'<div style="text-align:center;font-size:.75rem;color:{TEXT_SEC};'
    f'padding:12px;border-top:1px solid {BORDER}">'
    f'Fonte: Amazon BR via RapidAPI &nbsp;|&nbsp; '
    f'Pipeline: Collector → Redis → PostgreSQL → dbt → Streamlit &nbsp;|&nbsp; '
    f'Cache: 5 min'
    f'</div>',
    unsafe_allow_html=True,
)
