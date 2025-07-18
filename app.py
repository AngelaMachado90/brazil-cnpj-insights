import streamlit as st
import pytz
from datetime import datetime

# Configurações da página
st.set_page_config(
    page_title="Análise Avançada",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Configuração do fuso horário
fuso = pytz.timezone("America/Sao_Paulo")
agora = datetime.now(fuso).strftime("%d/%m/%Y %H:%M:%S")

# Ícones do Bootstrap
st.markdown("""
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap-icons@1.10.5/font/bootstrap-icons.css">
""", unsafe_allow_html=True)

st.title("Dashboard de Estabelecimentos Cadastrados")
st.markdown(f"<div style='text-align: right; color: #666;'><i class='bi bi-clock-history'></i> Última atualização: {agora}</div>", unsafe_allow_html=True)

st.markdown("""
Bem-vindo ao sistema de análise de dados do Mercado Livre de Energia e Cadastro CNPJ.

Navegue pelas páginas usando o menu lateral para acessar as diferentes análises disponíveis.
""")