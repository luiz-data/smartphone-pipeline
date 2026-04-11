"""
1_visao_geral.py — Visão Geral do Mercado de Smartphones BR.

Responde às perguntas de negócio:
  P1 — Qual é o preço médio, mínimo, máximo e mediano dos smartphones?
  P2 — Qual a proporção de produtos com frete grátis? Varia por condição?
  P6 — Como os smartphones estão distribuídos entre novo e usado?
       Qual o ticket médio por condição?
  P8 — Produtos com frete grátis têm preço médio maior ou menor que os sem frete?
  P9 — Qual a faixa de preço com maior concentração de produtos? (histograma R$500)
"""

import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from utils import (
    AMBER,
    BLUE,
    BRAND_COLORS,
    GREEN,
    PLOTLY_TEMPLATE,
    RED,
    TEXT_PRI,
    TEXT_SEC,
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
    page_title="Visão Geral — Smartphones BR",
    page_icon="📊",
    layout="wide",
)

st.markdown('<style>.block-container{padding-top:1.5rem}</style>', unsafe_allow_html=True)

# ── Sidebar ────────────────────────────────────────────────────────────────
filters = build_sidebar()
where   = build_where(filters)


# ══════════════════════════════════════════════════════════════════════════
# P1 — Indicadores de preço (KPI cards)
# ══════════════════════════════════════════════════════════════════════════
section_header("📊 Visão Geral do Mercado", "Indicadores agregados do período selecionado")

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
    cols = st.columns(5)
    with cols[0]:
        kpi_card("📦", "Total Produtos", fmt_int(row["total"]))
    with cols[1]:
        kpi_card("💰", "Preço Médio", fmt_brl(row["avg_price"]))
    with cols[2]:
        kpi_card("🏷️", "Preço Mínimo", fmt_brl(row["min_price"]))
    with cols[3]:
        kpi_card("🔝", "Preço Máximo", fmt_brl(row["max_price"]))
    with cols[4]:
        kpi_card("⚖️", "Mediana", fmt_brl(row["median_price"]))

    st.markdown("<br>", unsafe_allow_html=True)
    cols2 = st.columns(3)
    with cols2[0]:
        kpi_card("🚚", "Frete Grátis", fmt_pct(row["pct_free_shipping"]))
    with cols2[1]:
        kpi_card("⭐", "Avaliação Média", f"{float(row['avg_rating']):.2f}".replace(".", ",") if row["avg_rating"] else "—")
    with cols2[2]:
        kpi_card("🏷️", "Desconto Médio", fmt_pct(row["avg_discount"]))

    st.markdown("<br>", unsafe_allow_html=True)

    # IQR insight
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
section_header("🗂️ Distribuição por Faixa de Preço", "Concentração de produtos em buckets de R$500 (P9)")

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
        color_continuous_scale="Blues",
        template=PLOTLY_TEMPLATE,
        labels={"label": "Faixa de Preço", "total": "Produtos"},
    )
    fig_p9.update_traces(textposition="outside", marker_line_width=0)
    fig_p9.update_layout(
        coloraxis_showscale=False,
        xaxis_tickangle=-35,
        margin=dict(t=20, b=0),
        height=380,
    )
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
section_header("🔄 Distribuição por Condição", "Proporção e ticket médio por condição (P6)")

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
        color_map = {"new": BLUE, "used": AMBER}
        label_map = {"new": "Novo", "used": "Usado"}
        df_p6["label"] = df_p6["condition"].map(label_map).fillna(df_p6["condition"])
        colors = [color_map.get(c, "#94A3B8") for c in df_p6["condition"]]

        fig_p6 = px.pie(
            df_p6,
            names="label",
            values="total",
            color_discrete_sequence=colors,
            template=PLOTLY_TEMPLATE,
        )
        fig_p6.update_traces(
            textinfo="percent+label",
            hole=0.42,
            textfont_size=13,
        )
        fig_p6.update_layout(
            showlegend=False,
            margin=dict(t=10, b=10, l=10, r=10),
            height=320,
        )
        st.plotly_chart(fig_p6, use_container_width=True)

    with col_tbl:
        st.markdown("<br><br>", unsafe_allow_html=True)
        for _, r in df_p6.iterrows():
            lbl = label_map.get(r["condition"], r["condition"])
            st.markdown(
                f"""
                <div style="
                    background:#F8FAFC;border:1px solid #E2E8F0;
                    border-radius:10px;padding:14px 16px;margin-bottom:10px
                ">
                    <span style="font-weight:700;font-size:1rem">{lbl}</span>
                    <span style="float:right;font-weight:600;color:{TEXT_SEC}">{r['pct']:.1f}%</span><br>
                    <span style="font-size:.85rem;color:{TEXT_SEC}">
                        {fmt_int(r['total'])} produtos &nbsp;·&nbsp;
                        Média: {fmt_brl(r['avg_price'])} &nbsp;·&nbsp;
                        Mediana: {fmt_brl(r['median_price'])}
                    </span>
                </div>
                """,
                unsafe_allow_html=True,
            )

    # Insight de diferença de preço
    if len(df_p6) >= 2:
        new_row  = df_p6[df_p6["condition"] == "new"]
        used_row = df_p6[df_p6["condition"] == "used"]
        if not new_row.empty and not used_row.empty:
            diff = float(new_row["avg_price"].iloc[0]) - float(used_row["avg_price"].iloc[0])
            pct  = diff / float(used_row["avg_price"].iloc[0]) * 100
            insight_box(
                f"Smartphones novos custam em média {fmt_brl(diff)} ({pct:.1f}%) a mais que usados.",
                AMBER,
            )


# ══════════════════════════════════════════════════════════════════════════
# P2 + P8 — Frete grátis por condição e comparação de preço
# ══════════════════════════════════════════════════════════════════════════
st.markdown("<br>", unsafe_allow_html=True)
section_header("🚚 Frete Grátis", "Proporção por condição e impacto no preço (P2 · P8)")

col_left, col_right = st.columns(2)

# P2 — proporção por condição
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
            barmode="stack",
            text=df_p2["pct_within_condition"].apply(lambda v: f"{v:.1f}%".replace(".", ",")),
            color_discrete_map={"Frete Grátis": GREEN, "Frete Pago": "#CBD5E1"},
            template=PLOTLY_TEMPLATE,
            labels={
                "condition_label": "Condição",
                "pct_within_condition": "% dentro da condição",
                "frete_label": "",
            },
            title="Proporção por condição (P2)",
        )
        fig_p2.update_traces(textposition="inside", textfont_size=12)
        fig_p2.update_layout(
            margin=dict(t=40, b=0),
            height=320,
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        )
        st.plotly_chart(fig_p2, use_container_width=True)

# P8 — preço com vs sem frete grátis
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
        colors_p8 = [GREEN if v else "#CBD5E1" for v in df_p8["free_shipping"]]

        fig_p8 = go.Figure()
        for i, r in df_p8.iterrows():
            fig_p8.add_trace(go.Bar(
                x=[r["label"]],
                y=[float(r["avg_price"])],
                name=r["label"],
                text=[fmt_brl(r["avg_price"])],
                textposition="outside",
                marker_color=colors_p8[i],
            ))
        fig_p8.update_layout(
            template=PLOTLY_TEMPLATE,
            showlegend=False,
            title="Preço médio: frete grátis vs pago (P8)",
            yaxis_title="Preço médio (R$)",
            xaxis_title="",
            margin=dict(t=40, b=0),
            height=320,
        )
        st.plotly_chart(fig_p8, use_container_width=True)

        # Insight P8
        rows_dict = {bool(r["free_shipping"]): r for _, r in df_p8.iterrows()}
        if True in rows_dict and False in rows_dict:
            avg_free = float(rows_dict[True]["avg_price"])
            avg_paid = float(rows_dict[False]["avg_price"])
            diff = avg_free - avg_paid
            direction = "mais caro" if diff > 0 else "mais barato"
            insight_box(
                f"Produtos com frete grátis têm preço médio {fmt_brl(abs(diff))} "
                f"{direction} que produtos sem frete grátis.",
                GREEN if diff < 0 else AMBER,
            )
