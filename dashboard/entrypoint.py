import streamlit as st

st.set_page_config(
    page_title="Smartphones BR",
    layout="wide",
    initial_sidebar_state="expanded",
)

visao_geral = st.Page(
    "app.py",
    title="Visão Geral",
    icon=":material/dashboard:",
    default=True,
)

vendedores = st.Page(
    "pages/1_Vendedores.py",
    title="Vendedores",
    icon=":material/storefront:",
)

evolucao = st.Page(
    "pages/2_Evolucao.py",
    title="Evolução",
    icon=":material/trending_up:",
)

pg = st.navigation([visao_geral, vendedores, evolucao])
pg.run()
