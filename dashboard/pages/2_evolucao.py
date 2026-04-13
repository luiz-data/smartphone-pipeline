"""
2_evolucao.py — Evolução Temporal de Preços.

Responde às perguntas de negócio:
  P5 — Como os preços evoluíram ao longo do tempo?
       Existem tendências de alta ou baixa?
  P7 — Quais produtos tiveram maior variação de preço?
       Quais subiram e quais caíram mais?
"""

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
    BORDER,
    BORDER_GOLD,
    BG_CARD,
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
    page_title="Evolução de Preços — Smartphones BR",
    page_icon="📈",
    layout="wide",
)

# ── Sidebar (injeta CSS global) ────────────────────────────────────────────
filters  = build_sidebar()
where_fct = build_where(filters)


# ══════════════════════════════════════════════════════════════════════════
# P5 — Série temporal de preços
# ══════════════════════════════════════════════════════════════════════════
section_header("Evolução de Preços no Tempo", "Preço médio diário com banda de confiança P25–P75 (P5)", GOLD, "activity")

sql_p5 = f"""
    SELECT
        collection_date,
        avg_price,
        min_price,
        p25_price,
        p75_price,
        median_price,
        avg_discount_pct,
        avg_price_pct_change,
        total_observations,
        only_seed_data
    FROM marts.agg_price_evolution
    WHERE collection_date >= '{filters['cutoff_date']}'
    ORDER BY collection_date
"""
df_p5 = run_query(sql_p5)

if not check_empty(df_p5, "Sem dados de evolução temporal para o período selecionado."):
    has_seed = df_p5["only_seed_data"].any() if "only_seed_data" in df_p5.columns else False

    if has_seed:
        st.markdown(
            f"""
            <div class="alert-banner">
              <span class="alr-icon">{SVG["alert"]}</span>
              <span>
                <strong>Dados históricos sintéticos:</strong> Alguns dias exibidos contêm apenas
                dados de seed (histórico simulado para fins de demonstração do pipeline).
                Dias com dados reais estão indicados sem marcador especial.
              </span>
            </div>
            """,
            unsafe_allow_html=True,
        )

    fig_p5 = go.Figure()

    # Banda P25–P75
    fig_p5.add_trace(go.Scatter(
        x=list(df_p5["collection_date"]) + list(df_p5["collection_date"])[::-1],
        y=list(df_p5["p75_price"]) + list(df_p5["p25_price"])[::-1],
        fill="toself",
        fillcolor="rgba(201,168,76,0.08)",
        line=dict(color="rgba(0,0,0,0)"),
        name="P25–P75",
        hoverinfo="skip",
    ))

    # Linha de preço mínimo
    fig_p5.add_trace(go.Scatter(
        x=df_p5["collection_date"],
        y=df_p5["min_price"],
        mode="lines",
        line=dict(color=GREEN, dash="dot", width=1),
        name="Mínimo",
        opacity=0.7,
    ))

    # Linha principal (preço médio)
    marker_symbols = []
    marker_colors  = []
    for _, r in df_p5.iterrows():
        is_seed = r.get("only_seed_data", False)
        marker_symbols.append("diamond" if is_seed else "circle")
        marker_colors.append(AMBER if is_seed else GOLD)

    fig_p5.add_trace(go.Scatter(
        x=df_p5["collection_date"],
        y=df_p5["avg_price"],
        mode="lines+markers",
        line=dict(color=GOLD, width=2.5),
        marker=dict(size=7, color=marker_colors, symbol=marker_symbols),
        name="Média",
        customdata=df_p5[["avg_discount_pct", "total_observations", "only_seed_data"]].values,
        hovertemplate=(
            "<b>%{x}</b><br>"
            "Preço médio: R$%{y:,.2f}<br>"
            "Desconto médio: %{customdata[0]:.1f}%<br>"
            "Observações: %{customdata[1]}<br>"
            "%{customdata[2]}<extra></extra>"
        ),
    ))

    # Linha da mediana
    fig_p5.add_trace(go.Scatter(
        x=df_p5["collection_date"],
        y=df_p5["median_price"],
        mode="lines",
        line=dict(color=GOLD_LIGHT, dash="dash", width=1.5),
        name="Mediana",
        opacity=0.7,
    ))

    fig_p5.update_layout(
        **GRAPH_LAYOUT,
        template=PLOTLY_TEMPLATE,
        yaxis_title="Preço (R$)",
        xaxis_title="Data",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        margin=dict(t=20, b=0),
        height=360,
        hovermode="x unified",
    )
    fig_p5.update_xaxes(**AXIS_STYLE)
    fig_p5.update_yaxes(**AXIS_STYLE)
    st.plotly_chart(fig_p5, use_container_width=True)

    first_price = float(df_p5["avg_price"].iloc[0])
    last_price  = float(df_p5["avg_price"].iloc[-1])
    variation   = ((last_price - first_price) / first_price * 100) if first_price else 0
    direction   = "subiu" if variation > 0 else "caiu"
    color_ins   = RED if variation > 0 else GREEN

    insight_box(
        f"No período analisado, o preço médio {direction} "
        f"{fmt_pct(abs(variation))} — de {fmt_brl(first_price)} para {fmt_brl(last_price)}. "
        f"A banda P25–P75 mostra a dispersão dos preços; marcador losango indica dias com dados históricos sintéticos.",
        color_ins,
    )

    min_price_val = float(df_p5["min_price"].min())
    max_price_val = float(df_p5["avg_price"].max())
    amplitude     = max_price_val - min_price_val

    st.markdown("<br>", unsafe_allow_html=True)
    k1, k2, k3, k4 = st.columns(4)
    with k1:
        kpi_card(
            "calendar", "Dias Monitorados", fmt_int(len(df_p5)),
            icon_color="blue",
            back_insight="Cada ponto representa uma coleta de preços. Mais dias = tendências mais confiáveis para identificar sazonalidade e janelas de promoção.",
            back_comps=[
                {"label": "Período", "value": f"Últimos {filters['period_days']}d"},
                {"label": "Registros/dia", "value": fmt_int(int(df_p5['total_observations'].mean())) if 'total_observations' in df_p5.columns else "—"},
            ],
        )
    with k2:
        avg_chg = df_p5["avg_price_pct_change"].dropna()
        chg_val = float(avg_chg.mean()) if not avg_chg.empty else 0
        chg_direction = "positive" if chg_val < 0 else ("negative" if chg_val > 0 else "neutral")
        kpi_card(
            "trending_down" if chg_val < 0 else "trending_up",
            "Variação Média Diária",
            fmt_pct(chg_val) if not avg_chg.empty else "—",
            delta_type=chg_direction,
            icon_color="green" if chg_val < 0 else "red",
            back_insight="Variação média entre dias consecutivos. Valor próximo de zero indica estabilidade de preços — favorável para planejar a compra com antecedência.",
            back_comps=[
                {"label": "1ª observação", "value": fmt_brl(first_price)},
                {"label": "Última", "value": fmt_brl(last_price)},
            ],
        )
    with k3:
        kpi_card(
            "trending_down", "Menor Preço", fmt_brl(min_price_val),
            icon_color="green",
            back_insight="Menor preço observado em qualquer dia do período. Útil para identificar janelas de compra ideais e picos de promoção relâmpago.",
            back_comps=[
                {"label": "Maior preço", "value": fmt_brl(max_price_val)},
                {"label": "Amplitude", "value": fmt_brl(amplitude)},
            ],
        )
    with k4:
        kpi_card(
            "trending_up", "Maior Preço Médio", fmt_brl(max_price_val),
            icon_color="red",
            back_insight="Maior preço médio observado no período. A diferença entre máximo e mínimo revela a volatilidade do mercado — quanto maior, mais oportunidade para comprar no vale.",
            back_comps=[
                {"label": "Menor preço", "value": fmt_brl(min_price_val)},
                {"label": "Variação total", "value": fmt_pct(abs(variation))},
            ],
        )


# ══════════════════════════════════════════════════════════════════════════
# P7 — Variação de preço por produto
# ══════════════════════════════════════════════════════════════════════════
st.markdown("<br>", unsafe_allow_html=True)
section_header(
    "Variação de Preço por Produto",
    "Produtos com maior oscilação — tendência de alta, baixa ou estável (P7)",
    GOLD,
    "shuffle",
)

sql_p7 = f"""
    SELECT
        p.product_id,
        p.title,
        p.brand,
        v.first_price,
        v.last_price,
        v.price_variation_pct,
        v.max_drop_pct,
        v.price_trend,
        v.num_observations
    FROM marts.agg_price_variation v
    JOIN (
        SELECT DISTINCT ON (product_id) product_id, title, brand
        FROM marts.fct_products
        ORDER BY product_id, collected_at DESC
    ) p ON p.product_id = v.product_id
    WHERE v.num_observations >= 2
    ORDER BY ABS(v.price_variation_pct) DESC
    LIMIT 30
"""
df_p7 = run_query(sql_p7)

if not df_p7.empty and filters["brands"]:
    df_p7 = df_p7[df_p7["brand"].isin(filters["brands"])]

if not check_empty(df_p7, "Sem produtos com múltiplas observações de preço no período."):
    tab_tbl, tab_chart = st.tabs(["Tabela Interativa", "Gráfico de Variação"])

    with tab_tbl:
        df_show = df_p7.copy()
        # Trend labels com símbolo de direção
        trend_map = {
            "subiu":   ("↑ Alta",    RED),
            "caiu":    ("↓ Queda",   GREEN),
            "estável": ("→ Estável", TEXT_SEC),
        }
        df_show["title_short"] = df_show["title"].apply(
            lambda t: (t[:55] + "...") if isinstance(t, str) and len(t) > 55 else t
        )

        st.markdown(
            f"""
            <table style="width:100%;border-collapse:collapse;font-size:0.81rem;table-layout:fixed">
              <colgroup>
                <col style="width:22%">
                <col style="width:12%">
                <col style="width:12%">
                <col style="width:12%">
                <col style="width:10%">
                <col style="width:10%">
                <col style="width:14%">
                <col style="width:8%">
              </colgroup>
              <thead>
                <tr style="background:rgba(201,160,106,0.08);color:{GOLD};
                           font-size:0.7rem;text-transform:uppercase;letter-spacing:0.06em">
                  <th style="padding:10px 12px;text-align:left;border-bottom:1px solid {BORDER};white-space:nowrap;overflow:hidden;text-overflow:ellipsis">Produto</th>
                  <th style="padding:10px 12px;text-align:left;border-bottom:1px solid {BORDER};white-space:nowrap;overflow:hidden;text-overflow:ellipsis">Marca</th>
                  <th style="padding:10px 12px;text-align:right;border-bottom:1px solid {BORDER};white-space:nowrap;overflow:hidden;text-overflow:ellipsis">Inicial</th>
                  <th style="padding:10px 12px;text-align:right;border-bottom:1px solid {BORDER};white-space:nowrap;overflow:hidden;text-overflow:ellipsis">Final</th>
                  <th style="padding:10px 12px;text-align:right;border-bottom:1px solid {BORDER};white-space:nowrap;overflow:hidden;text-overflow:ellipsis">Variação</th>
                  <th style="padding:10px 12px;text-align:right;border-bottom:1px solid {BORDER};white-space:nowrap;overflow:hidden;text-overflow:ellipsis">Maior Queda</th>
                  <th style="padding:10px 12px;text-align:center;border-bottom:1px solid {BORDER};white-space:nowrap;overflow:hidden;text-overflow:ellipsis">Tendência</th>
                  <th style="padding:10px 12px;text-align:center;border-bottom:1px solid {BORDER};white-space:nowrap;overflow:hidden;text-overflow:ellipsis">Obs.</th>
                </tr>
              </thead>
              <tbody>
            """,
            unsafe_allow_html=True,
        )

        for idx, (_, r) in enumerate(df_show.iterrows()):
            var_pct   = float(r["price_variation_pct"]) if r["price_variation_pct"] is not None else 0
            trend_key = str(r["price_trend"])
            trend_lbl, trend_color = trend_map.get(trend_key, (trend_key, TEXT_SEC))
            var_str   = f"{'+'if var_pct > 0 else ''}{var_pct:.1f}%".replace(".", ",")
            drop_str  = f"-{float(r['max_drop_pct']):.1f}%".replace(".", ",") if r["max_drop_pct"] else "—"
            bg_row    = "rgba(0,0,0,0.015)" if idx % 2 == 0 else "transparent"
            var_color = RED if var_pct > 0 else (GREEN if var_pct < 0 else TEXT_SEC)

            st.markdown(
                f"""
                <tr style="border-bottom:1px solid {BORDER};background:{bg_row}">
                  <td style="padding:10px 12px;color:{TEXT_PRI};
                      overflow:hidden;text-overflow:ellipsis;white-space:nowrap">
                    {r['title_short']}
                  </td>
                  <td style="padding:10px 12px;color:{TEXT_SEC};white-space:nowrap">{r['brand'] or '—'}</td>
                  <td style="padding:10px 12px;text-align:right;color:{TEXT_SEC};white-space:nowrap">{fmt_brl(r['first_price'])}</td>
                  <td style="padding:10px 12px;text-align:right;color:{TEXT_PRI};white-space:nowrap">{fmt_brl(r['last_price'])}</td>
                  <td style="padding:10px 12px;text-align:right;font-weight:600;color:{var_color};white-space:nowrap">{var_str}</td>
                  <td style="padding:10px 12px;text-align:right;color:{GREEN};white-space:nowrap">{drop_str}</td>
                  <td style="padding:10px 12px;text-align:center;font-weight:700;
                      font-size:0.75rem;color:{trend_color};white-space:nowrap">{trend_lbl}</td>
                  <td style="padding:10px 12px;text-align:center;color:{TEXT_MUT};white-space:nowrap">{fmt_int(r['num_observations'])}</td>
                </tr>
                """,
                unsafe_allow_html=True,
            )
        st.markdown("</tbody></table>", unsafe_allow_html=True)

    with tab_chart:
        df_chart = df_p7.head(20).copy()
        df_chart["label"] = df_chart["title"].apply(
            lambda t: (t[:40] + "...") if isinstance(t, str) and len(t) > 40 else t
        )
        df_chart["var_pct"] = df_chart["price_variation_pct"].astype(float)
        df_chart = df_chart.sort_values("var_pct")

        colors_p7 = [RED if v > 0 else GREEN for v in df_chart["var_pct"]]

        fig_p7 = go.Figure(go.Bar(
            x=df_chart["var_pct"],
            y=df_chart["label"],
            orientation="h",
            text=df_chart["var_pct"].apply(
                lambda v: f"{'+'if v>0 else ''}{v:.1f}%".replace(".", ",")
            ),
            textposition="outside",
            marker_color=colors_p7,
            textfont=dict(color=TEXT_SEC, size=10),
        ))
        fig_p7.add_vline(x=0, line_width=1, line_dash="solid", line_color="rgba(0,0,0,0.1)")
        fig_p7.update_layout(
            **GRAPH_LAYOUT,
            template=PLOTLY_TEMPLATE,
            xaxis_title="Variação de Preço (%)",
            yaxis_title="",
            margin=dict(t=10, b=0, l=0, r=60),
            height=max(400, len(df_chart) * 28),
        )
        fig_p7.update_xaxes(**AXIS_STYLE)
        fig_p7.update_yaxes(**AXIS_STYLE)
        st.plotly_chart(fig_p7, use_container_width=True)

    n_caiu    = (df_p7["price_trend"] == "caiu").sum()
    n_subiu   = (df_p7["price_trend"] == "subiu").sum()
    n_estavel = (df_p7["price_trend"] == "estável").sum()
    total_p7  = len(df_p7)

    maior_queda = df_p7.loc[df_p7["price_variation_pct"].astype(float).idxmin()]
    insight_box(
        f"De {fmt_int(total_p7)} produtos analisados: "
        f"{n_caiu} tiveram queda ({fmt_pct(n_caiu/total_p7*100)}), "
        f"{n_subiu} tiveram alta ({fmt_pct(n_subiu/total_p7*100)}) e "
        f"{n_estavel} permaneceram estáveis. "
        f"Maior queda: {maior_queda['title'][:40]}... com "
        f"{float(maior_queda['price_variation_pct']):.1f}% de variação.",
        GREEN if n_caiu > n_subiu else RED,
    )
