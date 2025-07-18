import streamlit as st
import pandas as pd
import psycopg2
import plotly.express as px
from datetime import datetime, timedelta
import pytz
import time
import logging
from functools import wraps

# Configuração de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuração de constantes
class Config:
    TIMEZONE = "America/Sao_Paulo"
    DB_CONFIG = {
        "host": "emewe-mailling-db",
        "database": "cnpj_receita",
        "user": "postgres",
        "password": "postgres",
        "port": 5432
    }

# =============================================
# CONFIGURAÇÕES INICIAIS
# =============================================
st.set_page_config(
    page_title="Evolução do Mercado Livre de Energia",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Ícones Bootstrap
st.markdown(
    '<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap-icons@1.11.3/font/bootstrap-icons.min.css">',
    unsafe_allow_html=True
)

# =============================================
# FUNÇÕES UTILITÁRIAS
# =============================================

def corrigir_encoding(texto):
    """Corrige caracteres especiais mal formatados."""
    if pd.isna(texto):
        return "Não informado"
    
    # Primeiro tenta decodificar corretamente
    if isinstance(texto, str):
        try:
            # Tenta decodificar como latin1 e depois codificar para utf8
            return texto.encode('latin1').decode('utf-8')
        except:
            pass
    
    # Se ainda tiver problemas, aplica substituições específicas
    substituicoes = {
        'Ã\u008d': 'Í',
        'Ã\u0089': 'É',
        'Ã\u0087': 'Ç',
        'Ã\u0083': 'Ã',
        'Ã\u0081': 'Á',
        'Ã\u0095': 'Õ',
        'Ã\u008c': 'Ì',
        'Ã\u008e': 'Î',
        'ALIMENTÃ\u008dCIOS': 'ALIMENTÍCIOS',
        'COMÃ\u0089RCIO': 'COMÉRCIO',
        'EXTRAÃ\u0087Ã\u0083O': 'EXTRAÇÃO',
        'METÃ\u0081LICOS': 'METÁLICOS',
        'NÃ\u0083O-METÃ\u0081LICOS': 'NÃO-METÁLICOS',
        'QUÃ\u008dMICOS': 'QUÍMICOS',
        'SERVIÃ\u0087OS': 'SERVIÇOS',
        'TELECOMUNICAÃ\u0087Ã\u0095ES': 'TELECOMUNICAÇÕES',
        'VEÃ\u008dCULOS': 'VEÍCULOS'
    }
    
    texto_corrigido = str(texto)
    for original, substituicao in substituicoes.items():
        texto_corrigido = texto_corrigido.replace(original, substituicao)
    
    return texto_corrigido

@st.cache_resource
def get_db_connection():
    """Estabelece conexão com o banco de dados com encoding correto"""
    logger.info("Estabelecendo conexão com o banco de dados")
    conn = psycopg2.connect(**Config.DB_CONFIG)
    # Configura o encoding para UTF-8 na conexão
    conn.set_client_encoding('UTF8')
    return conn

def timing_decorator(func):
    """Decorador para medir e exibir tempo de execução."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        with st.spinner(f"Executando {func.__name__}..."):
            result = func(*args, **kwargs)
        
        elapsed_time = time.time() - start_time
        
        if elapsed_time > 1.0:
            st.toast(f"⏱️ {func.__name__} concluído em {elapsed_time:.2f}s", icon="✅")
        
        if "performance_logs" not in st.session_state:
            st.session_state.performance_logs = []
        st.session_state.performance_logs.append(
            f"{func.__name__}: {elapsed_time:.2f} segundos"
        )
        
        return result
    return wrapper

def format_milhar(n: int) -> str:
    """Formata número com separador de milhar."""
    return f"{n:,.0f}".replace(",", ".")

def get_current_time() -> str:
    """Retorna a data/hora atual formatada."""
    fuso = pytz.timezone(Config.TIMEZONE)
    return datetime.now(fuso).strftime("%d/%m/%Y %H:%M:%S")

# =============================================
# CARREGAMENTO DE DADOS
# =============================================

@st.cache_data(ttl=600, show_spinner="Carregando dados...")
def fetch_data(query: str, params=None) -> pd.DataFrame:
    """Executa consulta SQL e retorna DataFrame com cache"""
    try:
        with get_db_connection() as conn:
            logger.info(f"Executando consulta: {query[:100]}...")
            df = pd.read_sql(query, conn, params=params)
            
            # Aplica correção de encoding em colunas de texto
            for col in df.select_dtypes(include=['object']).columns:
                df[col] = df[col].apply(corrigir_encoding)
                
            return df
    except Exception as e:
        st.error(f"Erro ao executar consulta: {e}")
        logger.error(f"Erro na consulta: {str(e)}")
        return pd.DataFrame()

@timing_decorator
def load_filter_data():
    """Carrega dados para os filtros com tratamento de encoding"""
    try:
        # Consulta modificada para converter encoding diretamente no SQL
        cnaes_query = """
        SELECT DISTINCT 
            CASE 
                WHEN ramo_atividade IS NULL THEN 'Não informado'
                ELSE convert_from(convert_to(ramo_atividade, 'LATIN1'), 'UTF8')
            END AS ramo_atividade
        FROM ccee_parcela_carga_consumo_2025 
        WHERE ramo_atividade IS NOT NULL 
        ORDER BY ramo_atividade
        """
        
        ufs_query = """
        SELECT DISTINCT estado_uf 
        FROM ccee_parcela_carga_consumo_2025 
        WHERE estado_uf IS NOT NULL 
        ORDER BY estado_uf
        """
        
        perfis_query = """
        SELECT DISTINCT sigla_perfil_agente 
        FROM ccee_parcela_carga_consumo_2025 
        WHERE sigla_perfil_agente IS NOT NULL 
        ORDER BY sigla_perfil_agente
        """
        
        cnaes = fetch_data(cnaes_query)["ramo_atividade"].tolist()
        ufs = fetch_data(ufs_query)["estado_uf"].tolist()
        perfis = fetch_data(perfis_query)["sigla_perfil_agente"].tolist()
        total = fetch_data("SELECT COUNT(*) FROM ccee_parcela_carga_consumo_2025").iloc[0,0]
        
        return cnaes, ufs, perfis, total
    except Exception as e:
        st.error(f"Erro ao carregar dados de filtro: {e}")
        return [], [], [], 0

# =============================================
# INTERFACE PRINCIPAL
# =============================================

def main():
    tempo_inicio = time.time()
    
    st.title("📊 Análise de Migração para o Mercado Livre de Energia")
    st.caption(f"Dados atualizados em: {get_current_time()}")
    
    # Carrega dados para filtros
    cnaes_disponiveis, ufs_disponiveis, perfis_disponiveis, total_registros = load_filter_data()
    
    # Sidebar com filtros
    with st.sidebar:
        st.header("🔍 Filtros")
        
        col1, col2 = st.columns(2)
        with col1:
            data_inicio = st.date_input(
                "Data inicial",
                value=datetime.now() - timedelta(days=365),
                format="DD/MM/YYYY"
            )
        with col2:
            data_fim = st.date_input(
                "Data final",
                value=datetime.now(),
                format="DD/MM/YYYY"
            )
        
        if data_inicio > data_fim:
            st.error("A data inicial não pode ser maior que a data final")
            st.stop()
        
        tipo_consumidor = st.selectbox(
            "Tipo de Consumidor",
            ["Todos"] + perfis_disponiveis,
            index=0
        )
        
        cnae_selecionado = st.selectbox(
            "Ramo de Atividade",
            ["Todos"] + cnaes_disponiveis,
            index=0
        )
        
        uf_selecionada = st.selectbox(
            "UF",
            ["Todos"] + ufs_disponiveis,
            index=0
        )
        
        st.markdown("---")
        st.markdown(f"**Total de registros:** {format_milhar(total_registros)}")
    
    # Construção da consulta com filtros
    @timing_decorator
    def get_migration_data():
        query = """
            SELECT 
                data_migracao,
                CASE 
                    WHEN ramo_atividade IS NULL THEN 'Não informado'
                    ELSE convert_from(convert_to(ramo_atividade, 'LATIN1'), 'UTF8')
                END AS ramo_atividade,
                estado_uf,
                sigla_perfil_agente,
                COUNT(*) as total_migracoes,
                SUM(consumo_total) as consumo_total
            FROM ccee_parcela_carga_consumo_2025
            WHERE data_migracao BETWEEN %s AND %s
        """
        params = [data_inicio, data_fim]
        
        conditions = []
        
        if tipo_consumidor != "Todos":
            conditions.append("sigla_perfil_agente = %s")
            params.append(tipo_consumidor)
        
        if cnae_selecionado != "Todos":
            conditions.append("convert_from(convert_to(ramo_atividade, 'LATIN1'), 'UTF8') = %s")
            params.append(cnae_selecionado)
        
        if uf_selecionada != "Todos":
            conditions.append("estado_uf = %s")
            params.append(uf_selecionada)
        
        if conditions:
            query += " AND " + " AND ".join(conditions)
        
        query += " GROUP BY data_migracao, ramo_atividade, estado_uf, sigla_perfil_agente"
        
        return fetch_data(query, params)
    
    # Obtém os dados
    df_migracao = get_migration_data()
    
    if df_migracao.empty:
        st.warning("Nenhum dado encontrado com os filtros selecionados.")
        return
    
    # --- Visualizações ---
    st.subheader("📈 Evolução Temporal das Migrações")
    
    df_evolucao = df_migracao.groupby('data_migracao').agg({
        'total_migracoes': 'sum',
        'consumo_total': 'sum'
    }).reset_index()
    
    tab1, tab2 = st.tabs(["Quantidade de Migrações", "Volume de Consumo"])
    
    with tab1:
        fig_migracoes = px.line(
            df_evolucao,
            x='data_migracao',
            y='total_migracoes',
            title='Total de Migrações por Dia',
            labels={'data_migracao': 'Data', 'total_migracoes': 'Número de Migrações'}
        )
        st.plotly_chart(fig_migracoes, use_container_width=True)
    
    with tab2:
        fig_consumo = px.line(
            df_evolucao,
            x='data_migracao',
            y='consumo_total',
            title='Consumo Total Migrado (MWh)',
            labels={'data_migracao': 'Data', 'consumo_total': 'Consumo (MWh)'}
        )
        st.plotly_chart(fig_consumo, use_container_width=True)
    
    # --- Análise por segmento ---
    st.subheader("🔍 Análise por Segmento")
    
    segmento = st.radio(
        "Segmentar por:",
        ["Ramo de Atividade", "UF", "Tipo de Consumidor"],
        horizontal=True
    )
    
    if segmento == "Ramo de Atividade":
        col_segmento = 'ramo_atividade'
    elif segmento == "UF":
        col_segmento = 'estado_uf'
    else:
        col_segmento = 'sigla_perfil_agente'
    
    df_segmento = df_migracao.groupby(col_segmento).agg({
        'total_migracoes': 'sum',
        'consumo_total': 'sum'
    }).reset_index().sort_values('consumo_total', ascending=False)
    
    col1, col2 = st.columns([1, 2])
    
    with col1:
        st.dataframe(
            df_segmento,
            column_config={
                "total_migracoes": st.column_config.NumberColumn("Migrações", format="%d"),
                "consumo_total": st.column_config.NumberColumn("Consumo (MWh)", format="%.2f")
            },
            hide_index=True,
            use_container_width=True
        )
    
    with col2:
        fig_segmento = px.bar(
            df_segmento.head(10),
            x=col_segmento,
            y='consumo_total',
            title=f'Top 10 por Consumo - {segmento}',
            labels={col_segmento: segmento, 'consumo_total': 'Consumo (MWh)'}
        )
        st.plotly_chart(fig_segmento, use_container_width=True)
    
    # --- Dados detalhados ---
    st.subheader("📋 Dados Detalhados")
    
    csv = df_migracao.to_csv(index=False).encode('utf-8')
    
    st.download_button(
        label="📥 Baixar dados completos (CSV)",
        data=csv,
        file_name="dados_migracao_mercado_livre.csv",
        mime="text/csv"
    )
    
    st.dataframe(
        df_migracao,
        column_config={
            "data_migracao": st.column_config.DateColumn("Data Migração"),
            "total_migracoes": st.column_config.NumberColumn("Migrações", format="%d"),
            "consumo_total": st.column_config.NumberColumn("Consumo (MWh)", format="%.2f")
        },
        hide_index=True,
        use_container_width=True
    )

    # --- Rodapé ---
    st.markdown("---")
    st.markdown("""
    <div style="text-align: center; color: gray;">
        <strong>Fonte de Dados</strong><br>
        <ul style="list-style:none; padding:0;">
            <li><a href="https://www.ccee.org.br/portal/mercado-livre-de-energia" target="_blank">CCEE - Câmara de Comercialização de Energia Elétrica</a></li>
        </ul>
        <small>📅 Dados atualizados periodicamente
        
    </div>
    """, unsafe_allow_html=True)
    
    
    tempo_total = time.time() - tempo_inicio
    st.success(f"⏱️ Tempo total de carregamento: {tempo_total:.2f} segundos")

if __name__ == "__main__":
    main()