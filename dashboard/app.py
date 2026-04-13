"""
app.py — Ponto de entrada e Visão Geral do Dashboard Smartphones BR.

Responde às perguntas de negócio:
  P1 — Preço médio, mínimo, máximo e mediano dos smartphones
  P2 — Proporção de frete grátis por condição
  P6 — Distribuição novo × usado, ticket médio por condição
  P8 — Preço médio com vs sem frete grátis
  P9 — Histograma de faixas de preço (buckets R$500)
"""

import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from utils import (
    SVG,
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
    GRAPH_LAYOUT,
    PLOTLY_TEMPLATE,
    AXIS_STYLE,
    TEXT_PRI,
    TEXT_SEC,
    TEXT_MUT,
    build_sidebar,
    build_where,
    check_empty,
    fmt_brl,
    fmt_int,
    fmt_pct,
    insight_box,
    kpi_card,
    run_query,
    section_header,
)

st.set_page_config(
    page_title="Smartphones BR — Visão Geral",
    page_icon="📱",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
[data-testid="stSidebarNavItems"] li:first-child a span { visibility: hidden; }
[data-testid="stSidebarNavItems"] li:first-child a span::after { content: "Visão Geral"; visibility: visible; }
</style>
""", unsafe_allow_html=True)

# ── Sidebar (injeta CSS global) ────────────────────────────────────────────
filters = build_sidebar()
where   = build_where(filters)

# ── Hero bar ───────────────────────────────────────────────────────────────
st.markdown(
    f"""
    <div class="hero-bar">
      <div class="hero-left">
        <div class="hero-icon">{SVG["smartphone"]}</div>
        <div>
          <p class="hero-title">Smartphones BR</p>
          <p class="hero-sub">Monitoramento de preços — Amazon Brasil via RapidAPI</p>
        </div>
      </div>
      <div class="hero-right">
        <span class="hero-badge">Collector · Redis · PostgreSQL · dbt · Streamlit</span>
        <span class="hero-meta">Cache: 5 min</span>
      </div>
    </div>
    """,
    unsafe_allow_html=True,
)

# ══════════════════════════════════════════════════════════════════════════
# P1 — KPI cards (2 rows de 4)
# ══════════════════════════════════════════════════════════════════════════
section_header("Visão Geral do Mercado", "Indicadores agregados do período selecionado (P1)", GOLD, "pie_chart")

sql_p1 = f"""
    SELECT
        COUNT(*)                                        AS total,
        ROUND(AVG(price)::numeric, 2)                   AS avg_price,
        MIN(price)                                      AS min_price,
        MAX(price)                                      AS max_price,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price) AS median_price,
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY price) AS p25_price,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY price) AS p75_price,
        ROUND(
            100.0 * SUM(CASE WHEN free_shipping THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0),
            1
        )                                               AS pct_free_shipping,
        ROUND(AVG(rating)::numeric, 2)                  AS avg_rating,
        ROUND(AVG(discount_pct)::numeric, 1)            AS avg_discount
    FROM marts.fct_products
    {where}
"""
df_p1 = run_query(sql_p1)

if not check_empty(df_p1):
    row = df_p1.iloc[0]
    free_count = round(float(row["total"]) * float(row["pct_free_shipping"]) / 100) if row["pct_free_shipping"] else 0
    pct_diff   = ((float(row["avg_price"]) - float(row["median_price"])) / float(row["median_price"]) * 100) if row["median_price"] else 0

    # Linha 1
    r1c1, r1c2, r1c3, r1c4 = st.columns(4)
    with r1c1:
        kpi_card(
            "package", "Total Produtos", fmt_int(row["total"]),
            back_insight="Amazon BR lista milhares de smartphones. Nossa coleta captura os mais relevantes por volume de avaliações e relevância de busca.",
            back_comps=[
                {"label": "Com frete grátis", "value": fmt_int(free_count)},
                {"label": "Desconto médio", "value": fmt_pct(row["avg_discount"])},
            ],
        )
    with r1c2:
        kpi_card(
            "dollar", "Preço Médio", fmt_brl(row["avg_price"]),
            back_insight=f"Média influenciada pelo segmento premium. Mediana de {fmt_brl(row['median_price'])} representa melhor o mercado real.",
            back_comps=[
                {"label": "Mediana", "value": fmt_brl(row["median_price"])},
                {"label": "P25", "value": fmt_brl(row["p25_price"])},
            ],
        )
    with r1c3:
        kpi_card(
            "tag", "Preço Mínimo", fmt_brl(row["min_price"]),
            icon_color="green",
            back_insight="Produto mais barato da categoria. Faixas até R$999 dominam em volume, atraindo consumidores de entrada e usados recondicionados.",
            back_comps=[
                {"label": "P25", "value": fmt_brl(row["p25_price"])},
                {"label": "Mediana", "value": fmt_brl(row["median_price"])},
            ],
        )
    with r1c4:
        kpi_card(
            "trending_up", "Preço Máximo", fmt_brl(row["max_price"]),
            icon_color="red",
            back_insight="Extremidade premium do mercado. O segmento acima de R$5.000 é dominado por marcas como Apple — muito acima da mediana geral.",
            back_comps=[
                {"label": "P75", "value": fmt_brl(row["p75_price"])},
                {"label": "Mediana", "value": fmt_brl(row["median_price"])},
            ],
        )

    st.markdown("<br>", unsafe_allow_html=True)

    # Linha 2
    r2c1, r2c2, r2c3, r2c4 = st.columns(4)
    with r2c1:
        kpi_card(
            "layers", "Mediana", fmt_brl(row["median_price"]),
            icon_color="blue",
            back_insight=f"Valor que divide o catálogo ao meio — mais representativo que a média. Diferença à média: +{pct_diff:.1f}%.",
            back_comps=[
                {"label": "Média", "value": fmt_brl(row["avg_price"])},
                {"label": "P75", "value": fmt_brl(row["p75_price"])},
            ],
        )
    with r2c2:
        kpi_card(
            "truck", "Frete Grátis", fmt_pct(row["pct_free_shipping"]),
            icon_color="green",
            back_insight="Produtos com frete grátis geralmente têm ticket mais alto. Frete incluso é estratégia frequente no segmento premium.",
            back_comps=[
                {"label": "Com frete", "value": fmt_int(free_count)},
                {"label": "Desconto médio", "value": fmt_pct(row["avg_discount"])},
            ],
        )
    with r2c3:
        rating_str = f"{float(row['avg_rating']):.2f}".replace(".", ",") if row["avg_rating"] else "—"
        kpi_card(
            "star", "Avaliação Média", rating_str,
            back_insight="Rating consistente indica mercado maduro. Alta concentração de avaliações 4+ sugere produtos de qualidade razoável.",
            back_comps=[
                {"label": "Desconto médio", "value": fmt_pct(row["avg_discount"])},
                {"label": "Frete grátis", "value": fmt_pct(row["pct_free_shipping"])},
            ],
        )
    with r2c4:
        kpi_card(
            "percent", "Desconto Médio", fmt_pct(row["avg_discount"]),
            icon_color="purple",
            back_insight="Desconto médio modesto. Correlação fraca entre desconto e avaliações — mais desconto não gera necessariamente mais volume de vendas.",
            back_comps=[
                {"label": "Preço médio", "value": fmt_brl(row["avg_price"])},
                {"label": "Mediana", "value": fmt_brl(row["median_price"])},
            ],
        )

    st.markdown("<br>", unsafe_allow_html=True)

    if row["p25_price"] and row["p75_price"]:
        insight_box(
            f"50% dos smartphones estão entre {fmt_brl(row['p25_price'])} (P25) e "
            f"{fmt_brl(row['p75_price'])} (P75), com mediana de {fmt_brl(row['median_price'])}.",
            BLUE,
        )

# ══════════════════════════════════════════════════════════════════════════
# P9 — Histograma de faixas de preço (R$500)
# ══════════════════════════════════════════════════════════════════════════
st.markdown("<br>", unsafe_allow_html=True)
section_header("Distribuição por Faixa de Preço", "Concentração de produtos em buckets de R$500 (P9)", GOLD, "bar_chart")

sql_p9 = f"""
    SELECT
        price_bucket_500_start                              AS bucket_start,
        price_bucket_500_end                                AS bucket_end,
        COUNT(*)                                            AS total,
        ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct
    FROM marts.fct_products
    {where}
    GROUP BY price_bucket_500_start, price_bucket_500_end
    ORDER BY price_bucket_500_start
"""
df_p9 = run_query(sql_p9)

if not check_empty(df_p9):
    df_p9["label"] = df_p9.apply(
        lambda r: f"R${int(r['bucket_start']):,}–R${int(r['bucket_end']):,}".replace(",", "."),
        axis=1,
    )
    peak = df_p9.loc[df_p9["total"].idxmax()]

    fig_p9 = px.bar(
        df_p9,
        x="label",
        y="total",
        text=df_p9["pct"].apply(lambda v: f"{v:.1f}%".replace(".", ",")),
        color="total",
        color_continuous_scale=[[0, "rgba(201,168,76,0.3)"], [1, GOLD]],
        template=PLOTLY_TEMPLATE,
        labels={"label": "Faixa de Preço", "total": "Produtos"},
    )
    fig_p9.update_traces(textposition="outside", marker_line_width=0)
    fig_p9.update_layout(
        **GRAPH_LAYOUT,
        coloraxis_showscale=False,
        xaxis_tickangle=-35,
        margin=dict(t=20, b=0),
        height=320,
    )
    fig_p9.update_xaxes(**AXIS_STYLE)
    fig_p9.update_yaxes(**AXIS_STYLE)
    st.plotly_chart(fig_p9, use_container_width=True)
    insight_box(
        f"A faixa {peak['label']} concentra {fmt_int(peak['total'])} produtos "
        f"({peak['pct']:.1f}% do total) — maior concentração do mercado.",
        BLUE,
    )


# ══════════════════════════════════════════════════════════════════════════
# P6 — Distribuição novo × usado
# ══════════════════════════════════════════════════════════════════════════
st.markdown("<br>", unsafe_allow_html=True)
section_header("Distribuição por Condição", "Proporção e ticket médio por condição (P6)", GOLD, "refresh")

sql_p6 = f"""
    SELECT
        condition,
        COUNT(*)                                            AS total,
        ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct,
        ROUND(AVG(price)::numeric, 2)                       AS avg_price,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price)  AS median_price
    FROM marts.fct_products
    {where}
    GROUP BY condition
    ORDER BY total DESC
"""
df_p6 = run_query(sql_p6)

if not check_empty(df_p6):
    col_pie, col_tbl = st.columns([1, 1])

    with col_pie:
        color_map = {"new": BLUE, "used": GOLD}
        label_map = {"new": "Novo", "used": "Usado"}
        df_p6["label"] = df_p6["condition"].map(label_map).fillna(df_p6["condition"])
        colors = [color_map.get(c, "#4a5568") for c in df_p6["condition"]]

        fig_p6 = px.pie(
            df_p6,
            names="label",
            values="total",
            color_discrete_sequence=colors,
            template=PLOTLY_TEMPLATE,
        )
        fig_p6.update_traces(
            textinfo="percent+label",
            hole=0.48,
            textfont_size=13,
        )
        fig_p6.update_layout(
            **GRAPH_LAYOUT,
            showlegend=False,
            margin=dict(t=10, b=10, l=10, r=10),
            height=300,
        )
        st.plotly_chart(fig_p6, use_container_width=True)

    with col_tbl:
        st.markdown("<br>", unsafe_allow_html=True)
        for _, r in df_p6.iterrows():
            lbl    = label_map.get(r["condition"], r["condition"])
            accent = color_map.get(r["condition"], BLUE)
            st.markdown(
                f"""
                <div style="
                    background:{BG_CARD};
                    border:1px solid {BORDER};
                    border-left:3px solid {accent};
                    border-radius:12px;
                    padding:16px 18px;
                    margin-bottom:10px;
                    box-shadow:0 2px 12px rgba(0,0,0,0.06);
                    backdrop-filter:blur(20px);
                ">
                    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:6px">
                      <span style="font-weight:700;font-size:0.95rem;color:{TEXT_PRI}">{lbl}</span>
                      <span style="font-weight:700;font-size:1.0rem;color:{accent}">{r['pct']:.1f}%</span>
                    </div>
                    <div style="font-size:0.78rem;color:{TEXT_SEC}">
                        {fmt_int(r['total'])} produtos
                        &nbsp;·&nbsp; Média: {fmt_brl(r['avg_price'])}
                        &nbsp;·&nbsp; Mediana: {fmt_brl(r['median_price'])}
                    </div>
                </div>
                """,
                unsafe_allow_html=True,
            )

    if len(df_p6) >= 2:
        new_row  = df_p6[df_p6["condition"] == "new"]
        used_row = df_p6[df_p6["condition"] == "used"]
        if not new_row.empty and not used_row.empty:
            diff = float(new_row["avg_price"].iloc[0]) - float(used_row["avg_price"].iloc[0])
            pct  = diff / float(used_row["avg_price"].iloc[0]) * 100
            insight_box(
                f"Smartphones novos custam em média {fmt_brl(diff)} ({pct:.1f}%) a mais que usados.",
                GOLD,
            )


# ══════════════════════════════════════════════════════════════════════════
# P2 + P8 — Frete grátis por condição e comparação de preço
# ══════════════════════════════════════════════════════════════════════════
st.markdown("<br>", unsafe_allow_html=True)
section_header("Frete Grátis", "Proporção por condição e impacto no preço (P2 · P8)", GREEN, "truck")

col_left, col_right = st.columns([1, 1.4])

with col_left:
    sql_p2 = f"""
        SELECT
            condition,
            free_shipping,
            COUNT(*)                                            AS total,
            ROUND(
                100.0 * COUNT(*) /
                SUM(COUNT(*)) OVER (PARTITION BY condition),
                1
            )                                                   AS pct_within_condition
        FROM marts.fct_products
        {where}
        GROUP BY condition, free_shipping
        ORDER BY condition, free_shipping DESC
    """
    df_p2 = run_query(sql_p2)

    if not check_empty(df_p2, "Sem dados de frete para os filtros selecionados."):
        label_map = {"new": "Novo", "used": "Usado"}
        df_p2["condition_label"] = df_p2["condition"].map(label_map).fillna(df_p2["condition"])
        df_p2["frete_label"] = df_p2["free_shipping"].map({True: "Frete Grátis", False: "Frete Pago"})

        fig_p2 = px.bar(
            df_p2,
            x="condition_label",
            y="pct_within_condition",
            color="frete_label",
            barmode="group",
            color_discrete_map={
                "Frete Grátis": "#52b788",
                "Frete Pago": "rgba(0,0,0,0.12)",
            },
            template=PLOTLY_TEMPLATE,
            labels={
                "condition_label": "",
                "pct_within_condition": "%",
                "frete_label": "",
            },
        )
        fig_p2.update_traces(
            texttemplate="%{y:.1f}%",
            textposition="inside",
            textfont=dict(size=11, color="#ffffff"),
            width=0.4,
        )
        fig_p2.update_layout(
            **GRAPH_LAYOUT,
            title=dict(
                text="Proporção com frete grátis por condição",
                font=dict(size=11, color="#4a4b5a"),
                x=0,
                pad=dict(l=0, t=0),
            ),
            bargap=0.3,
            bargroupgap=0.05,
            height=320,
            margin=dict(t=36, b=0, l=0, r=0),
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.05,
                xanchor="right",
                x=1,
                font=dict(size=10, color="#4a4b5a"),
                bgcolor="rgba(0,0,0,0)",
                borderwidth=0,
            ),
            yaxis=dict(
                title="Proporção (%)",
                title_font=dict(size=10, color="#9a9aaa"),
                ticksuffix="%",
                tickfont=dict(size=9, color="#9a9aaa"),
                gridcolor="rgba(0,0,0,0.05)",
                linecolor="rgba(0,0,0,0.08)",
                showticklabels=True,
            ),
            xaxis=dict(
                title="",
                tickfont=dict(size=10, color="#4a4b5a"),
                linecolor="rgba(0,0,0,0.08)",
            ),
        )
        fig_p2.update_xaxes(**AXIS_STYLE)
        fig_p2.update_yaxes(**AXIS_STYLE)
        st.plotly_chart(fig_p2, use_container_width=True)

with col_right:
    sql_p8 = f"""
        SELECT
            free_shipping,
            ROUND(AVG(price)::numeric, 2)                       AS avg_price,
            COUNT(*)                                            AS total,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price)  AS median_price
        FROM marts.fct_products
        {where}
        GROUP BY free_shipping
        ORDER BY free_shipping DESC
    """
    df_p8 = run_query(sql_p8)

    if not check_empty(df_p8, "Sem dados para comparação de preço."):
        df_p8["label"] = df_p8["free_shipping"].map({True: "Frete Grátis", False: "Frete Pago"})
        colors_p8 = ["#52b788" if v else "rgba(0,0,0,0.1)" for v in df_p8["free_shipping"]]

        fig_p8 = go.Figure()
        for i, r in df_p8.iterrows():
            cor = "#52b788" if r["free_shipping"] else "rgba(0,0,0,0.12)"
            texto = fmt_brl(float(r["avg_price"]))
            fig_p8.add_trace(go.Bar(
                x=[r["label"]],
                y=[float(r["avg_price"])],
                name=r["label"],
                text=[texto],
                textposition="inside",
                textfont=dict(
                    size=10,
                    color="#ffffff" if r["free_shipping"] else "#4a4b5a",
                ),
                marker_color=cor,
                width=0.35,
            ))
        fig_p8.update_layout(
            **GRAPH_LAYOUT,
            template=PLOTLY_TEMPLATE,
            showlegend=False,
            title=dict(
                text="Preço médio: frete grátis vs pago",
                font=dict(size=11, color="#4a4b5a"),
                x=0,
                pad=dict(l=0, t=0),
            ),
            bargap=0.55,
            height=320,
            margin=dict(t=36, b=0, l=0, r=0),
            yaxis=dict(
                title="Preço médio (R$)",
                title_font=dict(size=10, color="#9a9aaa"),
                tickprefix="R$ ",
                tickfont=dict(size=9, color="#9a9aaa"),
                gridcolor="rgba(0,0,0,0.05)",
                linecolor="rgba(0,0,0,0.08)",
                showticklabels=True,
            ),
            xaxis=dict(
                title="",
                tickfont=dict(size=11, color="#4a4b5a"),
                linecolor="rgba(0,0,0,0.08)",
            ),
        )
        fig_p8.update_xaxes(**AXIS_STYLE)
        fig_p8.update_yaxes(**AXIS_STYLE)
        st.plotly_chart(fig_p8, use_container_width=True)

        rows_dict = {bool(r["free_shipping"]): r for _, r in df_p8.iterrows()}
        if True in rows_dict and False in rows_dict:
            avg_free = float(rows_dict[True]["avg_price"])
            avg_paid = float(rows_dict[False]["avg_price"])
            diff = avg_free - avg_paid
            direction = "mais caro" if diff > 0 else "mais barato"
            insight_box(
                f"Produtos com frete grátis têm preço médio {fmt_brl(abs(diff))} "
                f"{direction} que produtos sem frete grátis.",
                GREEN if diff < 0 else GOLD,
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
