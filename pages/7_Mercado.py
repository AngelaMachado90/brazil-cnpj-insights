import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime

# Configuração da página
st.set_page_config(page_title="Dashboard Migrações", layout="wide")

# Título
st.title("Evolução de Migrações no Mercado Livre")

# Carregar dados (simulação - substitua pelo seu DataFrame real)
@st.cache_data
def load_data():
    # Aqui você carregaria seus dados reais
    # Exemplo com dados simulados baseados na estrutura dos documentos
    dates = pd.date_range(start="2023-01-01", end="2024-12-31", freq='M')
    data = {
        'mes_referencia': dates,
        'case_varietta_concuritato': [100 + i*10 + i**2 for i in range(len(dates))],
        'case_lists_peril': [80 + i*8 + (i*1.5)**2 for i in range(len(dates))],
        'case_sports_peril': [120 + i*5 + i**1.5 for i in range(len(dates))],
        'case_pancels_carga_consumo': [90 + i*12 + i**1.8 for i in range(len(dates))],
        'estado': ['SP' if i%2 else 'RJ' for i in range(len(dates))],
        'cidade': ['São Paulo' if i%2 else 'Rio de Janeiro' for i in range(len(dates))]
    }
    return pd.DataFrame(data)

df = load_data()

# Filtros
col1, col2, col3 = st.columns(3)
with col1:
    start_date = st.date_input("Data inicial", value=datetime(2023, 1, 1), 
                              min_value=datetime(2023, 1, 1), 
                              max_value=datetime(2024, 12, 31))
with col2:
    end_date = st.date_input("Data final", value=datetime(2024, 12, 31), 
                            min_value=datetime(2023, 1, 1), 
                            max_value=datetime(2024, 12, 31))
with col3:
    estados = st.multiselect("Estados", options=df['estado'].unique(), default=df['estado'].unique())

# Filtrar dados
df_filtered = df[(df['mes_referencia'].dt.date >= start_date) & 
                 (df['mes_referencia'].dt.date <= end_date) &
                 (df['estado'].isin(estados))]

# Gráfico 1: Evolução das migrações por tipo de caso
st.subheader("Evolução das Migrações por Tipo de Caso")
fig1 = px.line(df_filtered, x='mes_referencia', y=['case_varietta_concuritato', 'case_lists_peril', 
                                                  'case_sports_peril', 'case_pancels_carga_consumo'],
              labels={'value': 'Número de Migrações', 'mes_referencia': 'Mês de Referência', 'variable': 'Tipo de Caso'},
              title='Evolução Temporal das Migrações')
st.plotly_chart(fig1, use_container_width=True)

# Gráfico 2: Comparação por estado
st.subheader("Comparação por Estado")
fig2 = px.bar(df_filtered.groupby(['estado', 'mes_referencia']).sum().reset_index(), 
             x='mes_referencia', y='case_varietta_concuritato', color='estado',
             labels={'case_varietta_concuritato': 'Migrações Varietta', 'mes_referencia': 'Mês de Referência'},
             title='Migrações Varietta por Estado')
st.plotly_chart(fig2, use_container_width=True)

# Métricas
st.subheader("Métricas Principais")
col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Varietta", df_filtered['case_varietta_concuritato'].sum())
col2.metric("Total Lists Peril", df_filtered['case_lists_peril'].sum())
col3.metric("Total Sports Peril", df_filtered['case_sports_peril'].sum())
col4.metric("Total Carga Consumo", df_filtered['case_pancels_carga_consumo'].sum())

# Tabela com detalhes
st.subheader("Detalhes das Migrações")
st.dataframe(df_filtered.sort_values('mes_referencia', ascending=False))