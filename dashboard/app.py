"""
app.py — Ponto de entrada do dashboard Smartphones BR.

Configura a página, verifica a conexão com o banco de dados e exibe
a landing page com resumo do mercado e links de navegação.
"""

import streamlit as st
from utils import (
    SVG,
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
    TEXT_MUT,
    GOLD,
    GOLD_LIGHT,
    BLUE,
    GREEN,
    RED,
    AMBER,
    PURPLE,
    BORDER,
    BORDER_GOLD,
    BG_CARD,
    BG_SURFACE,
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
    st.error(f"Sem conexão com o PostgreSQL: {exc}")

# ── Hero ───────────────────────────────────────────────────────────────────
st.markdown(
    f"""
    <div style="
        position: relative;
        overflow: hidden;
        text-align: center;
        padding: 48px 24px 40px;
        background: linear-gradient(135deg, #0e0e1a 0%, #1a1530 50%, #0e0e1a 100%);
        border-radius: 20px;
        margin-bottom: 32px;
        border: 1px solid {BORDER_GOLD};
        box-shadow: 0 8px 48px rgba(0,0,0,0.6);
    ">
      <!-- decorative circle -->
      <div style="
          position: absolute;
          top: -60px; right: -60px;
          width: 200px; height: 200px;
          border-radius: 50%;
          background: radial-gradient(circle, rgba(201,168,76,0.12) 0%, transparent 70%);
          pointer-events: none;
      "></div>
      <div style="
          position: absolute;
          bottom: -40px; left: -40px;
          width: 160px; height: 160px;
          border-radius: 50%;
          background: radial-gradient(circle, rgba(74,122,255,0.08) 0%, transparent 70%);
          pointer-events: none;
      "></div>

      <!-- smartphone SVG icon -->
      <div style="
          display: inline-flex;
          align-items: center;
          justify-content: center;
          width: 64px; height: 64px;
          background: rgba(201,168,76,0.12);
          border: 1px solid {BORDER_GOLD};
          border-radius: 18px;
          color: #c9a84c;
          margin-bottom: 18px;
      ">
        <svg xmlns="http://www.w3.org/2000/svg" width="32" height="32" viewBox="0 0 24 24"
             fill="none" stroke="currentColor" stroke-width="1.5"
             stroke-linecap="round" stroke-linejoin="round">
          <rect x="5" y="2" width="14" height="20" rx="2" ry="2"/>
          <line x1="12" y1="18" x2="12.01" y2="18"/>
        </svg>
      </div>

      <h1 style="
          color: #f5f5f7;
          font-size: 2.1rem;
          font-weight: 800;
          margin: 0 0 8px 0;
          letter-spacing: -0.03em;
      ">Smartphones BR</h1>

      <p style="color: #888899; font-size: 0.95rem; margin: 0 0 18px 0;">
        Monitoramento de preços na Amazon Brasil via RapidAPI
      </p>

      <div style="
          display: inline-flex;
          align-items: center;
          gap: 6px;
          background: rgba(201,168,76,0.08);
          border: 1px solid {BORDER_GOLD};
          border-radius: 20px;
          padding: 5px 18px;
          font-size: 0.75rem;
          color: #c9a84c;
          font-weight: 500;
          letter-spacing: 0.04em;
      ">
        Collector · Redis · PostgreSQL · dbt · Streamlit
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
            kpi_card("package", "Produtos", fmt_int(row["total_produtos"]), warm=True)
        with c2:
            kpi_card("dollar", "Preço Médio", fmt_brl(row["preco_medio"]), warm=True)
        with c3:
            kpi_card("tag", "Menor Preço", fmt_brl(row["preco_min"]), icon_color="green")
        with c4:
            kpi_card("award", "Marcas", fmt_int(row["total_marcas"]), icon_color="purple")
        with c5:
            kpi_card("truck", "Frete Grátis", fmt_pct(row["pct_frete_gratis"]), icon_color="blue")
    else:
        st.info("Nenhum dado disponível para os filtros selecionados.")

    st.markdown("<br>", unsafe_allow_html=True)

# ── Cards de navegação ─────────────────────────────────────────────────────
st.markdown(
    f'<div style="font-size:0.68rem;font-weight:600;text-transform:uppercase;'
    f'letter-spacing:0.08em;color:{TEXT_MUT};margin-bottom:16px">'
    f'Páginas do Dashboard</div>',
    unsafe_allow_html=True,
)

nav_cols = st.columns(3)

nav_cards = [
    {
        "svg": SVG["pie_chart"],
        "title": "Visão Geral do Mercado",
        "desc": "Preços, distribuição por condição, frete grátis e histograma de faixas de preço.",
        "questions": "P1 · P2 · P6 · P8 · P9",
        "color": BLUE,
        "bg": "rgba(74,122,255,0.08)",
    },
    {
        "svg": SVG["award"],
        "title": "Marcas & Competitividade",
        "desc": "Ranking de marcas por volume, dispersão desconto × avaliações e score de competitividade.",
        "questions": "P3 · P4 · P10",
        "color": GOLD,
        "bg": "rgba(201,168,76,0.08)",
    },
    {
        "svg": SVG["activity"],
        "title": "Evolução de Preços",
        "desc": "Série temporal de preços com banda de confiança e variação por produto.",
        "questions": "P5 · P7",
        "color": GREEN,
        "bg": "rgba(0,200,150,0.08)",
    },
]

for col, card in zip(nav_cols, nav_cards):
    with col:
        st.markdown(
            f"""
            <div style="
                background: {BG_CARD};
                border: 1px solid {BORDER};
                border-top: 2px solid {card['color']};
                border-radius: 16px;
                padding: 24px 20px;
                min-height: 190px;
                box-shadow: 0 4px 24px rgba(0,0,0,0.4);
                transition: transform 0.2s ease, box-shadow 0.2s ease;
                overflow: hidden;
                backdrop-filter: blur(20px);
            ">
              <div style="
                  display: inline-flex;
                  align-items: center;
                  justify-content: center;
                  width: 40px; height: 40px;
                  background: {card['bg']};
                  border-radius: 10px;
                  color: {card['color']};
                  margin-bottom: 14px;
              ">{card['svg']}</div>
              <div style="font-size:0.95rem;font-weight:700;color:{TEXT_PRI};margin-bottom:8px">
                {card['title']}
              </div>
              <div style="font-size:0.78rem;color:{TEXT_SEC};line-height:1.5;margin-bottom:12px">
                {card['desc']}
              </div>
              <div style="
                  display: inline-block;
                  background: {card['bg']};
                  color: {card['color']};
                  font-size: 0.68rem;
                  font-weight: 700;
                  letter-spacing: 0.06em;
                  border-radius: 20px;
                  padding: 3px 12px;
                  border: 1px solid {card['color']}44;
              ">{card['questions']}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

# ── Rodapé ─────────────────────────────────────────────────────────────────
st.markdown("<br>", unsafe_allow_html=True)
st.markdown(
    f'<div style="text-align:center;font-size:0.7rem;color:{TEXT_MUT};'
    f'padding:14px;border-top:1px solid {BORDER}">'
    f'Fonte: Amazon BR via RapidAPI &nbsp;·&nbsp; '
    f'Pipeline: Collector → Redis → PostgreSQL → dbt → Streamlit &nbsp;·&nbsp; '
    f'Cache: 5 min'
    f'</div>',
    unsafe_allow_html=True,
)
