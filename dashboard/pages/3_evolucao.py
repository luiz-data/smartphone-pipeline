"""
3_evolucao.py — Evolução Temporal de Preços.

Responde às perguntas de negócio:
  P5 — Como os preços evoluíram ao longo do tempo?
       Existem tendências de alta ou baixa?
  P7 — Quais produtos tiveram maior variação de preço?
       Quais subiram e quais caíram mais?
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
    page_title="Evolução de Preços — Smartphones BR",
    page_icon="📈",
    layout="wide",
)

st.markdown('<style>.block-container{padding-top:1.5rem}</style>', unsafe_allow_html=True)

# ── Sidebar ────────────────────────────────────────────────────────────────
filters = build_sidebar()
where_fct = build_where(filters)


# ══════════════════════════════════════════════════════════════════════════
# P5 — Série temporal de preços
# ══════════════════════════════════════════════════════════════════════════
section_header("📈 Evolução de Preços no Tempo", "Preço médio diário com banda de confiança P25–P75 (P5)")

sql_p5 = f"""
    SELECT
        day,
        avg_price,
        min_price,
        max_price,
        p25_price,
        p75_price,
        median_price,
        avg_discount_pct,
        avg_price_pct_change,
        total_observations,
        only_seed_data
    FROM marts.agg_price_evolution
    WHERE day >= '{filters['cutoff_date']}'
    ORDER BY day
"""
df_p5 = run_query(sql_p5)

if not check_empty(df_p5, "Sem dados de evolução temporal para o período selecionado."):
    has_seed = df_p5["only_seed_data"].any() if "only_seed_data" in df_p5.columns else False

    # Banner de aviso quando há dados semente
    if has_seed:
        st.markdown(
            f"""
            <div style="
                background:{AMBER}18;border-left:4px solid {AMBER};
                padding:10px 14px;border-radius:0 8px 8px 0;
                font-size:.85rem;color:{TEXT_PRI};margin-bottom:12px
            ">
                ⚠️ <strong>Dados históricos sintéticos:</strong> Alguns dias exibidos contêm apenas
                dados de seed (histórico simulado para fins de demonstração do pipeline).
                Dias com dados reais estão indicados sem asterisco.
            </div>
            """,
            unsafe_allow_html=True,
        )

    fig_p5 = go.Figure()

    # Banda P25–P75
    fig_p5.add_trace(go.Scatter(
        x=list(df_p5["day"]) + list(df_p5["day"])[::-1],
        y=list(df_p5["p75_price"]) + list(df_p5["p25_price"])[::-1],
        fill="toself",
        fillcolor=f"{BLUE}22",
        line=dict(color="rgba(0,0,0,0)"),
        name="P25–P75",
        hoverinfo="skip",
    ))

    # Linha de preço mínimo
    fig_p5.add_trace(go.Scatter(
        x=df_p5["day"],
        y=df_p5["min_price"],
        mode="lines",
        line=dict(color=GREEN, dash="dot", width=1),
        name="Mínimo",
        opacity=0.6,
    ))

    # Linha de preço máximo
    fig_p5.add_trace(go.Scatter(
        x=df_p5["day"],
        y=df_p5["max_price"],
        mode="lines",
        line=dict(color=RED, dash="dot", width=1),
        name="Máximo",
        opacity=0.6,
    ))

    # Linha principal (preço médio)
    marker_symbols = []
    marker_colors  = []
    for _, r in df_p5.iterrows():
        is_seed = r.get("only_seed_data", False)
        marker_symbols.append("diamond" if is_seed else "circle")
        marker_colors.append(AMBER if is_seed else BLUE)

    fig_p5.add_trace(go.Scatter(
        x=df_p5["day"],
        y=df_p5["avg_price"],
        mode="lines+markers",
        line=dict(color=BLUE, width=2.5),
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
        x=df_p5["day"],
        y=df_p5["median_price"],
        mode="lines",
        line=dict(color="#7C3AED", dash="dash", width=1.5),
        name="Mediana",
    ))

    fig_p5.update_layout(
        template=PLOTLY_TEMPLATE,
        yaxis_title="Preço (R$)",
        xaxis_title="Data",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        margin=dict(t=20, b=0),
        height=400,
        hovermode="x unified",
    )
    st.plotly_chart(fig_p5, use_container_width=True)

    # Insight automático
    first_price = float(df_p5["avg_price"].iloc[0])
    last_price  = float(df_p5["avg_price"].iloc[-1])
    variation   = ((last_price - first_price) / first_price * 100) if first_price else 0
    direction   = "subiu" if variation > 0 else "caiu"
    color_ins   = RED if variation > 0 else GREEN

    insight_box(
        f"No período analisado, o preço médio {direction} "
        f"{fmt_pct(abs(variation))} — de {fmt_brl(first_price)} para {fmt_brl(last_price)}. "
        f"A banda P25–P75 mostra a dispersão dos preços; ◆ indica dias com dados históricos sintéticos.",
        color_ins,
    )

    # KPIs do período
    st.markdown("<br>", unsafe_allow_html=True)
    k1, k2, k3, k4 = st.columns(4)
    with k1:
        kpi_card("📅", "Dias Monitorados", fmt_int(len(df_p5)))
    with k2:
        avg_chg = df_p5["avg_price_pct_change"].dropna()
        kpi_card(
            "📉", "Variação Média Diária",
            fmt_pct(float(avg_chg.mean())) if not avg_chg.empty else "—",
            delta_color=GREEN if (not avg_chg.empty and float(avg_chg.mean()) < 0) else RED,
        )
    with k3:
        kpi_card("🔻", "Menor Preço Registrado", fmt_brl(float(df_p5["min_price"].min())))
    with k4:
        kpi_card("🔺", "Maior Preço Registrado", fmt_brl(float(df_p5["max_price"].max())))


# ══════════════════════════════════════════════════════════════════════════
# P7 — Variação de preço por produto
# ══════════════════════════════════════════════════════════════════════════
st.markdown("<br>", unsafe_allow_html=True)
section_header(
    "🔀 Variação de Preço por Produto",
    "Produtos com maior oscilação — tendência de alta, baixa ou estável (P7)",
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

# Aplica filtros de marca da sidebar no resultado
if not df_p7.empty and filters["brands"]:
    df_p7 = df_p7[df_p7["brand"].isin(filters["brands"])]

if not check_empty(df_p7, "Sem produtos com múltiplas observações de preço no período."):
    # ── Tabs: tabela interativa + gráfico de waterfall ─────────────────────
    tab_tbl, tab_chart = st.tabs(["📋 Tabela Interativa", "📊 Gráfico de Variação"])

    with tab_tbl:
        # Ordena por variação absoluta
        df_show = df_p7.copy()
        df_show["trend_icon"] = df_show["price_trend"].map(
            {"subiu": "↑ Subiu", "caiu": "↓ Caiu", "estável": "→ Estável"}
        ).fillna(df_show["price_trend"])

        df_show["title_short"] = df_show["title"].apply(
            lambda t: (t[:55] + "...") if isinstance(t, str) and len(t) > 55 else t
        )

        # Renderiza tabela HTML estilizada
        st.markdown(
            f"""
            <table style="width:100%;border-collapse:collapse;font-size:.83rem">
              <thead>
                <tr style="background:#F1F5F9;color:{TEXT_SEC};font-size:.73rem;text-transform:uppercase">
                  <th style="padding:8px 10px;text-align:left">Produto</th>
                  <th style="padding:8px 10px;text-align:left">Marca</th>
                  <th style="padding:8px 10px;text-align:right">Preço Inicial</th>
                  <th style="padding:8px 10px;text-align:right">Preço Final</th>
                  <th style="padding:8px 10px;text-align:right">Variação</th>
                  <th style="padding:8px 10px;text-align:right">Maior Queda</th>
                  <th style="padding:8px 10px;text-align:center">Tendência</th>
                  <th style="padding:8px 10px;text-align:right">Observações</th>
                </tr>
              </thead>
              <tbody>
            """,
            unsafe_allow_html=True,
        )

        for _, r in df_show.iterrows():
            var_pct = float(r["price_variation_pct"]) if r["price_variation_pct"] is not None else 0
            trend   = str(r["trend_icon"])
            trend_color = (
                RED if "Subiu" in trend else
                GREEN if "Caiu" in trend else
                AMBER
            )
            var_str  = f"{'+'if var_pct > 0 else ''}{var_pct:.1f}%".replace(".", ",")
            drop_str = f"-{float(r['max_drop_pct']):.1f}%".replace(".", ",") if r["max_drop_pct"] else "—"

            st.markdown(
                f"""
                <tr style="border-bottom:1px solid #F1F5F9">
                  <td style="padding:7px 10px;color:{TEXT_PRI};max-width:220px;
                      overflow:hidden;text-overflow:ellipsis;white-space:nowrap">
                    {r['title_short']}
                  </td>
                  <td style="padding:7px 10px;color:{TEXT_SEC}">{r['brand'] or '—'}</td>
                  <td style="padding:7px 10px;text-align:right">{fmt_brl(r['first_price'])}</td>
                  <td style="padding:7px 10px;text-align:right">{fmt_brl(r['last_price'])}</td>
                  <td style="padding:7px 10px;text-align:right;font-weight:600;
                      color:{'#DC2626' if var_pct > 0 else '#16A34A' if var_pct < 0 else TEXT_SEC}">
                    {var_str}
                  </td>
                  <td style="padding:7px 10px;text-align:right;color:#16A34A">{drop_str}</td>
                  <td style="padding:7px 10px;text-align:center;font-weight:600;
                      color:{trend_color}">{trend}</td>
                  <td style="padding:7px 10px;text-align:right;color:{TEXT_SEC}">{fmt_int(r['num_observations'])}</td>
                </tr>
                """,
                unsafe_allow_html=True,
            )
        st.markdown("</tbody></table>", unsafe_allow_html=True)

    with tab_chart:
        # Gráfico de barras horizontal — variação %
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
        ))
        fig_p7.add_vline(x=0, line_width=1, line_dash="solid", line_color="#94A3B8")
        fig_p7.update_layout(
            template=PLOTLY_TEMPLATE,
            xaxis_title="Variação de Preço (%)",
            yaxis_title="",
            margin=dict(t=10, b=0, l=0, r=60),
            height=max(320, len(df_chart) * 26),
        )
        st.plotly_chart(fig_p7, use_container_width=True)

    # Insight P7
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
