"""
2_vendedores.py — Marcas & Competitividade.

Responde às perguntas de negócio:
  P3  — Quais as 10 marcas mais vendidas (por volume de avaliações / vendas)?
  P4  — Existe correlação entre desconto e volume de vendas/avaliações?
  P10 — Qual marca oferece melhor custo-benefício (preço × qualidade)?
"""

import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from utils import (
    SVG,
    GOLD,
    GOLD_LIGHT,
    GOLD_MUTED,
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
    page_title="Marcas & Competitividade — Smartphones BR",
    page_icon="🏆",
    layout="wide",
)

# ── Sidebar (injeta CSS global) ────────────────────────────────────────────
filters = build_sidebar()
where   = build_where(filters)


# ══════════════════════════════════════════════════════════════════════════
# P3 — Top 10 marcas por volume
# ══════════════════════════════════════════════════════════════════════════
section_header("Ranking de Marcas", "Top 10 por volume de avaliações e produtos (P3)", GOLD, "award")

sql_p3 = f"""
    SELECT
        COALESCE(brand, 'Outros')                           AS brand,
        COUNT(*)                                            AS total_produtos,
        SUM(num_ratings)                                    AS total_avaliacoes,
        ROUND(AVG(price)::numeric, 2)                       AS avg_price,
        ROUND(AVG(rating)::numeric, 2)                      AS avg_rating,
        ROUND(AVG(discount_pct)::numeric, 1)                AS avg_discount,
        ROUND(
            100.0 * SUM(CASE WHEN free_shipping THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0),
            1
        )                                                   AS pct_free_shipping
    FROM marts.fct_products
    {where}
    GROUP BY brand
    ORDER BY total_avaliacoes DESC NULLS LAST
    LIMIT 10
"""
df_p3 = run_query(sql_p3)

if not check_empty(df_p3):
    col_tbl, col_bar = st.columns([1, 1])

    with col_tbl:
        # Header da tabela
        st.markdown(
            f"""
            <table style="width:100%;border-collapse:collapse;font-size:0.82rem;table-layout:fixed">
              <colgroup>
                <col style="width:44px">
                <col style="width:130px">
                <col style="width:90px">
                <col style="width:110px">
                <col style="width:120px">
                <col style="width:90px">
              </colgroup>
              <thead>
                <tr style="background:rgba(201,168,76,0.06);color:{TEXT_SEC};
                           font-size:0.7rem;text-transform:uppercase;letter-spacing:0.06em">
                  <th style="padding:10px 12px;text-align:left;border-bottom:1px solid {BORDER};white-space:nowrap">#</th>
                  <th style="padding:10px 12px;text-align:left;border-bottom:1px solid {BORDER};white-space:nowrap">Marca</th>
                  <th style="padding:10px 12px;text-align:right;border-bottom:1px solid {BORDER};white-space:nowrap">Produtos</th>
                  <th style="padding:10px 12px;text-align:right;border-bottom:1px solid {BORDER};white-space:nowrap">Avaliações</th>
                  <th style="padding:10px 12px;text-align:right;border-bottom:1px solid {BORDER};white-space:nowrap">Preço Médio</th>
                  <th style="padding:10px 12px;text-align:right;border-bottom:1px solid {BORDER};white-space:nowrap">Rating</th>
                </tr>
              </thead>
              <tbody>
            """,
            unsafe_allow_html=True,
        )
        rank_labels = {1: "01", 2: "02", 3: "03"}
        rank_colors = {1: GOLD, 2: "#888899", 3: AMBER}
        for i, r in df_p3.iterrows():
            rank = i + 1
            rank_str   = rank_labels.get(rank, f"{rank:02d}")
            rank_color = rank_colors.get(rank, TEXT_MUT)
            bg_row = "rgba(201,168,76,0.03)" if rank % 2 == 0 else "transparent"
            rating_str = f"{str(r['avg_rating']).replace('.', ',')} ★" if r["avg_rating"] else "—"
            st.markdown(
                f"""
                <tr style="border-bottom:1px solid {BORDER};background:{bg_row}">
                  <td style="padding:9px 12px;color:{rank_color};font-weight:700;font-size:0.78rem;white-space:nowrap">{rank_str}</td>
                  <td style="padding:9px 12px;font-weight:600;color:{TEXT_PRI};white-space:nowrap;overflow:hidden;text-overflow:ellipsis">{r['brand']}</td>
                  <td style="padding:9px 12px;text-align:right;color:{TEXT_SEC};white-space:nowrap">{fmt_int(r['total_produtos'])}</td>
                  <td style="padding:9px 12px;text-align:right;color:{TEXT_PRI};font-weight:500;white-space:nowrap">{fmt_int(r['total_avaliacoes'])}</td>
                  <td style="padding:9px 12px;text-align:right;color:{TEXT_SEC};white-space:nowrap">{fmt_brl(r['avg_price'])}</td>
                  <td style="padding:9px 12px;text-align:right;color:{GOLD};white-space:nowrap">{rating_str}</td>
                </tr>
                """,
                unsafe_allow_html=True,
            )
        st.markdown("</tbody></table>", unsafe_allow_html=True)

    with col_bar:
        df_p3_sorted = df_p3.sort_values("total_avaliacoes")
        fig_p3 = px.bar(
            df_p3_sorted,
            x="total_avaliacoes",
            y="brand",
            orientation="h",
            text=df_p3_sorted["total_avaliacoes"].apply(fmt_int),
            color="total_avaliacoes",
            color_continuous_scale=[[0, "rgba(201,168,76,0.3)"], [1, GOLD]],
            template=PLOTLY_TEMPLATE,
            labels={"total_avaliacoes": "Total de Avaliações", "brand": "Marca"},
        )
        fig_p3.update_traces(textposition="outside", marker_line_width=0, textfont_color=TEXT_SEC)
        fig_p3.update_layout(
            **GRAPH_LAYOUT,
            coloraxis_showscale=False,
            margin=dict(t=10, b=0, l=0, r=70),
            height=380,
            xaxis_title="Total de Avaliações",
            yaxis_title="",
        )
        fig_p3.update_xaxes(**AXIS_STYLE)
        fig_p3.update_yaxes(**AXIS_STYLE)
        st.plotly_chart(fig_p3, use_container_width=True)

    top = df_p3.iloc[0]
    insight_box(
        f"{top['brand']} lidera com {fmt_int(top['total_avaliacoes'])} avaliações e "
        f"{fmt_int(top['total_produtos'])} produtos — preço médio de {fmt_brl(top['avg_price'])}.",
        BLUE,
    )


# ══════════════════════════════════════════════════════════════════════════
# P4 — Correlação desconto × volume
# ══════════════════════════════════════════════════════════════════════════
st.markdown("<br>", unsafe_allow_html=True)
section_header(
    "Desconto × Volume de Avaliações",
    "Existe correlação entre desconto oferecido e volume de vendas/avaliações? (P4)",
    BLUE,
    "percent",
)

sql_p4_scatter = f"""
    SELECT
        COALESCE(brand, 'Outros')   AS brand,
        ROUND(discount_pct::numeric, 1) AS discount_pct,
        num_ratings,
        price,
        rating,
        condition
    FROM marts.fct_products
    {where}
    AND discount_pct IS NOT NULL
    AND num_ratings > 0
    ORDER BY num_ratings DESC
    LIMIT 500
"""
df_p4 = run_query(sql_p4_scatter)

sql_p4_corr = f"""
    SELECT
        ROUND(CORR(discount_pct, num_ratings)::numeric, 3) AS pearson_desconto_avaliacoes
    FROM marts.fct_products
    {where}
    AND discount_pct IS NOT NULL
    AND num_ratings > 0
"""
df_corr = run_query(sql_p4_corr)

if not check_empty(df_p4, "Sem dados suficientes para análise de correlação."):
    pearson = None
    if not df_corr.empty:
        pearson = df_corr["pearson_desconto_avaliacoes"].iloc[0]

    label_map = {"new": "Novo", "used": "Usado"}
    df_p4["condition_label"] = df_p4["condition"].map(label_map).fillna(df_p4["condition"])

    fig_p4 = px.scatter(
        df_p4,
        x="discount_pct",
        y="num_ratings",
        color="condition_label",
        size="price",
        size_max=20,
        hover_name="brand",
        hover_data={
            "discount_pct": ":.1f",
            "num_ratings": ":,",
            "price": ":,.0f",
            "condition_label": False,
        },
        color_discrete_map={"Novo": BLUE, "Usado": GOLD},
        template=PLOTLY_TEMPLATE,
        labels={
            "discount_pct": "Desconto (%)",
            "num_ratings": "Nº de Avaliações",
            "condition_label": "Condição",
        },
    )
    # Linha de tendência manual via média móvel
    if len(df_p4) >= 5:
        sorted_df = df_p4.sort_values("discount_pct")
        window = min(5, len(sorted_df))
        trend_y = np.convolve(sorted_df["num_ratings"], np.ones(window) / window, mode="valid")
        trend_x = sorted_df["discount_pct"].iloc[window - 1:].values
        fig_p4.add_trace(go.Scatter(
            x=trend_x,
            y=trend_y,
            mode="lines",
            line=dict(color="rgba(201,168,76,0.6)", width=2, dash="dash"),
            name="Tendência",
        ))

    fig_p4.update_layout(
        **GRAPH_LAYOUT,
        margin=dict(t=10, b=0),
        height=400,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )
    fig_p4.update_xaxes(**AXIS_STYLE)
    fig_p4.update_yaxes(**AXIS_STYLE)
    st.plotly_chart(fig_p4, use_container_width=True)

    if pearson is not None:
        p_val = float(pearson)
        strength = (
            "forte positiva" if p_val > 0.5 else
            "moderada positiva" if p_val > 0.2 else
            "fraca positiva" if p_val > 0 else
            "forte negativa" if p_val < -0.5 else
            "moderada negativa" if p_val < -0.2 else
            "fraca negativa"
        )
        color = GREEN if p_val > 0.2 else (RED if p_val < -0.2 else GOLD)
        insight_box(
            f"Correlação de Pearson entre desconto e avaliações: r = {str(pearson).replace('.', ',')} "
            f"— correlação {strength}. "
            f"{'Maiores descontos tendem a gerar mais avaliações.' if p_val > 0.2 else 'Não há evidência de que descontos maiores aumentem avaliações.'} "
            f"(Tamanho do ponto = preço do produto)",
            color,
        )


# ══════════════════════════════════════════════════════════════════════════
# P10 — Score de custo-benefício por marca (bubble chart)
# ══════════════════════════════════════════════════════════════════════════
st.markdown("<br>", unsafe_allow_html=True)
section_header(
    "Custo-Benefício por Marca",
    "Preço médio × avaliação média, tamanho da bolha = competitividade (P10)",
    GREEN,
    "gem",
)

sql_p10 = f"""
    SELECT
        brand,
        avg_price,
        avg_rating,
        total_products,
        total_reviews,
        competitiveness_score,
        rank_by_competitiveness
    FROM marts.dim_sellers
    WHERE brand IS NOT NULL
      AND total_products >= 2
    ORDER BY rank_by_competitiveness
    LIMIT 15
"""
df_p10 = run_query(sql_p10)

if not check_empty(df_p10, "Sem dados de custo-benefício disponíveis (mínimo 2 produtos por marca)."):
    if filters["brands"]:
        df_p10 = df_p10[df_p10["brand"].isin(filters["brands"])]

    if check_empty(df_p10, "Sem dados para as marcas selecionadas."):
        pass
    else:
        fig_p10 = px.scatter(
            df_p10,
            x="avg_price",
            y="avg_rating",
            size="competitiveness_score",
            color="brand",
            text="brand",
            size_max=60,
            hover_data={
                "avg_price": ":,.2f",
                "avg_rating": ":.2f",
                "total_products": ":,",
                "total_reviews": ":,",
                "competitiveness_score": ":.3f",
                "brand": False,
            },
            color_discrete_sequence=BRAND_COLORS,
            template=PLOTLY_TEMPLATE,
            labels={
                "avg_price": "Preço Médio (R$)",
                "avg_rating": "Avaliação Média",
                "brand": "Marca",
            },
        )
        fig_p10.update_traces(
            textposition="top center",
            textfont_size=11,
            marker_line_width=1,
            marker_line_color=BORDER_GOLD,
        )
        fig_p10.update_layout(
            **GRAPH_LAYOUT,
            showlegend=False,
            margin=dict(t=20, b=0),
            height=460,
        )
        fig_p10.update_xaxes(**AXIS_STYLE)
        fig_p10.update_yaxes(**AXIS_STYLE)
        st.plotly_chart(fig_p10, use_container_width=True)

        best = df_p10.iloc[0]
        insight_box(
            f"{best['brand']} tem o melhor score de competitividade "
            f"({str(round(float(best['competitiveness_score']), 3)).replace('.', ',')}), "
            f"combinando preço médio de {fmt_brl(best['avg_price'])} com "
            f"avaliação média de {str(round(float(best['avg_rating']), 2)).replace('.', ',')} estrelas. "
            f"Score = (avaliação × avaliações) ÷ preço.",
            GREEN,
        )
