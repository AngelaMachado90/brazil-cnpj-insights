"""
Dashboard de Enriquecimento de Dados RFB + CCEE

Este aplicativo fornece uma análise estratégica das empresas que migraram para o Mercado Livre de Energia,
integrando dados da Receita Federal do Brasil (RFB) e da Câmara de Comercialização de Energia Elétrica (CCEE).

Principais funcionalidades:
- Análise temporal da migração (1-10 anos)
- Filtros por localização (UF/município), setor (CNAE) e porte
- Detalhamento completo por empresa (CNPJ, contatos, sócios)
- Visualização gráfica interativa
- Exportação de dados em CSV
- Enriquecimento de dados via web scraping
- Monitoramento em tempo real

Requisitos técnicos:
- Python 3.9+
- PostgreSQL 12+
- Syt
- Bibliotecas listadas em requirements.txt

Arquitetura:
- Frontend: Streamlit
- Backend: Python (Pandas, Plotly, Psycopg2)
- Banco de dados: PostgreSQL
- Web scraping: BeautifulSoup/Selenium (script externo)

Desenvolvido por:
Angela Machado | Julho 2025 | Versão 3.0
"""

# =============================================
# IMPORTAÇÕES
# =============================================
import os
import streamlit as st
from streamlit.components.v1 import html
import pandas as pd
import psycopg2
import plotly.express as px
from datetime import datetime, timedelta
import pytz
import time
import subprocess
from contextlib import contextmanager
from functools import wraps
import logging
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, ColumnsAutoSizeMode


# =============================================
# CONFIGURAÇÃO INICIAL
# =============================================

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("dashboard.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Configuração do Streamlit
st.set_page_config(
    page_title="Dashboard Migração Mercado Livre",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={
        'Get Help': 'https://github.com/seuuser/seurepo',
        'Report a bug': "https://github.com/seuuser/seurepo/issues",
        'About': "### Painel estratégico para análise de migração ao Mercado Livre de Energia"
    }
)

# =============================================
# CONSTANTES E CONFIGURAÇÕES
# =============================================

# Integracao com o bootstrap icone + botoes
st.markdown("""
<link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap-icons@1.11.3/font/bootstrap-icons.min.css">
<script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/js/bootstrap.bundle.min.js"></script>
<style>
    .btn-custom {
        margin: 5px;
        padding: 10px 15px;
        border-radius: 8px;
    }
    .icon-header {
        font-size: 1.2em;
        margin-right: 8px;
    }
</style>
""", unsafe_allow_html=True)

class Config:
    """Classe centralizada para configurações do aplicativo"""
    
    # Configurações de timezone
    TIMEZONE = "America/Sao_Paulo"
    DATE_FORMAT = "%d/%m/%Y %H:%M:%S"
    
    # Configurações do banco de dados
    DB_CONFIG = {
        "host": "emewe-mailling-db",
        "database": "cnpj_receita",
        "user": "postgres",
        "password": "postgres",
        "port": 5432,
        "connect_timeout": 5
    }
    
    # Configurações de caminhos
    SCRAPING_SCRIPT_PATH = os.path.join(
        os.path.dirname(__file__), 
        "utils", 
        "scraping", 
        "extrair_contatos.py"
    )
    
    # Configurações de cache
    CACHE_TTL = 3600  # 1 hora em segundos

# Início da contagem para monitoramento de performance
APP_START_TIME = time.time()

# =============================================
# FUNÇÕES UTILITÁRIAS
# =============================================

def format_milhar(n: int) -> str:
    """
    Formata número com separador de milhar.
    
    Args:
        n: Número inteiro a ser formatado
        
    Returns:
        String formatada com separador de milhar (ex: 1.000)
    """
    return f"{n:,.0f}".replace(",", ".")

def format_cnpj(cnpj: str) -> str:
    """
    Formata CNPJ com máscara padrão (XX.XXX.XXX/XXXX-XX).
    
    Args:
        cnpj: String contendo o CNPJ (com ou sem formatação)
        
    Returns:
        String com CNPJ formatado
    """
    cnpj = str(cnpj).zfill(14)
    return f"{cnpj[:2]}.{cnpj[2:5]}.{cnpj[5:8]}/{cnpj[8:12]}-{cnpj[12:14]}"

def get_current_time() -> str:
    """
    Obtém a data/hora atual formatada conforme timezone configurado.
    
    Returns:
        String com data/hora formatada
    """
    fuso = pytz.timezone(Config.TIMEZONE)
    return datetime.now(fuso).strftime(Config.DATE_FORMAT)

@contextmanager
def get_db_connection():
    """
    Gerenciador de contexto para conexões com o banco de dados.
    
    Yields:
        Objeto de conexão com o banco de dados
        
    Raises:
        Exception: Erro ao conectar ao banco de dados
    """
    conn = None
    try:
        conn = psycopg2.connect(**Config.DB_CONFIG)
        conn.set_client_encoding('UTF8')
        yield conn
    except Exception as e:
        logger.error(f"Erro na conexão com o banco: {str(e)}")
        st.error("Erro ao conectar ao banco de dados")
        raise
    finally:
        if conn is not None:
            conn.close()

def timing_decorator(func):
    """
    Decorador para medir e registrar tempo de execução de funções.
    
    Args:
        func: Função a ser decorada
        
    Returns:
        Função decorada com medição de tempo
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        logger.info(f"Iniciando execução de {func.__name__}")
        
        with st.spinner(f"Executando {func.__name__}..."):
            result = func(*args, **kwargs)
        
        elapsed_time = time.time() - start_time
        logger.info(f"{func.__name__} concluído em {elapsed_time:.2f}s")
        
        if elapsed_time > 1.0:
            st.toast(f"⏱️ {func.__name__} concluído em {elapsed_time:.2f}s", icon="✅")
        
        if "performance_logs" not in st.session_state:
            st.session_state.performance_logs = []
        st.session_state.performance_logs.append(
            f"{func.__name__}: {elapsed_time:.2f} segundos"
        )
        
        return result
    return wrapper

# =============================================
# FUNÇÕES DE ACESSO A DADOS
# =============================================

@st.cache_data(ttl=600, show_spinner="Carregando dados...")
def fetch_data(query: str, params: tuple = None) -> pd.DataFrame:
    """
    Executa consulta SQL e retorna DataFrame.
    
    Args:
        query: String com a consulta SQL
        params: Parâmetros para consulta parametrizada
        
    Returns:
        DataFrame com resultados da consulta
        
    Raises:
        Exception: Erro ao executar consulta
    """
    try:
        with get_db_connection() as conn:
            return pd.read_sql(query, conn, params=params)
    except Exception as e:
        logger.error(f"Erro na consulta: {query[:100]}... - {str(e)}")
        st.error(f"Erro ao executar consulta: {str(e)}")
        return pd.DataFrame()

@st.cache_data(ttl=Config.CACHE_TTL)
@timing_decorator
def get_static_data() -> dict:
    """
    Carrega dados estáticos para filtros e configurações iniciais.
    
    Returns:
        Dicionário com:
        - ufs: Lista de UFs disponíveis
        - municipios: DataFrame com municípios
        - cnaes: DataFrame com códigos CNAE
        - anos: Lista de anos disponíveis
    """
    data = {
        "ufs": [],
        "municipios": pd.DataFrame(),
        "cnaes": pd.DataFrame(),
        "anos": []
    }
    
    try:
        # Carrega UFs
        data["ufs"] = fetch_data(
            "SELECT DISTINCT uf FROM rfb_estabelecimentos ORDER BY uf"
        )["uf"].dropna().tolist()
        
        # Carrega municípios
        data["municipios"] = fetch_data(
            "SELECT * FROM vw_municipios_com_estabelecimentos ORDER BY descricao"
        )
        
        # Carrega CNAEs
        data["cnaes"] = fetch_data(
            "SELECT codigo, descricao FROM cnae_10 ORDER BY codigo"
        )
        
        # Carrega anos disponíveis
        anos_df = fetch_data("""
            SELECT DISTINCT EXTRACT(YEAR FROM data_situacao_cadastral) AS ano
            FROM vw_estabelecimentos_empresas
            WHERE data_situacao_cadastral IS NOT NULL
            ORDER BY ano DESC
        """)
        
        if not anos_df.empty:
            data["anos"] = anos_df['ano'].dropna().astype(int).tolist()
            
    except Exception as e:
        logger.error(f"Erro ao carregar dados estáticos: {str(e)}")
        st.error(f"Erro ao carregar dados estáticos: {str(e)}")
        
    return data

def initialize_session_state():
    """Inicializa ou reinicializa o estado da sessão."""
    default_state = {
        'dados_carregados': False,
        'carregamento_iniciado': False,
        'static_data': None,
        'df_migracao': None,
        'df_empresas': None,
        'data_importacao': None,
        'total_empresas': None,
        'cnae_options': ["Todos"],
        'performance_logs': []
    }
    
    for key, value in default_state.items():
        if key not in st.session_state:
            st.session_state[key] = value
@st.cache_data(ttl=Config.CACHE_TTL)
@timing_decorator
def query_migracao_por_periodo(anos: int = 5) -> pd.DataFrame:
    """
    Consulta migrações agregadas por período mensal.
    
    Args:
        anos: Número de anos para análise retroativa
        
    Returns:
        DataFrame com colunas:
        - ano_mes: Período no formato YYYY-MM
        - quantidade_migracoes: Número de migrações no período
    """
    try:
        query = """
        WITH dados_migracao AS (
            SELECT 
                TO_CHAR(data_migracao, 'YYYY-MM') AS ano_mes,
                COUNT(DISTINCT cnpj_carga) AS quantidade_migracoes,
                0 AS eh_total
            FROM ccee_parcela_carga_consumo_2025
            WHERE data_migracao IS NOT NULL
            AND data_migracao >= (CURRENT_DATE - INTERVAL %s)
            GROUP BY TO_CHAR(data_migracao, 'YYYY-MM')
            
            UNION ALL
            
            SELECT 
                'TOTAL' AS ano_mes,
                COUNT(DISTINCT cnpj_carga) AS quantidade_migracoes,
                1 AS eh_total
            FROM ccee_parcela_carga_consumo_2025
            WHERE data_migracao IS NOT NULL
            AND data_migracao >= (CURRENT_DATE - INTERVAL %s)
        )
        SELECT ano_mes, quantidade_migracoes
        FROM dados_migracao
        ORDER BY eh_total, ano_mes;
        """
        
        return fetch_data(query, (f"{anos} years", f"{anos} years"))
        
    except Exception as e:
        logger.error(f"Erro ao consultar migrações por período: {str(e)}")
        st.error("Erro ao carregar dados de migração")
        return pd.DataFrame()

@st.cache_data(ttl=Config.CACHE_TTL)
@timing_decorator
def query_empresas_migradas(anos: int = 5) -> pd.DataFrame:
    """
    Consulta empresas que migraram para o mercado livre.
    
    Args:
        anos: Número de anos para análise retroativa
        
    Returns:
        DataFrame com colunas:
        - CNPJ, nome_fantasia, DATA_MIGRACAO, ANO_MES, uf, municipio
        - cnae_fiscal_principal, cnae_descricao, EMAIL, TELEFONE01, SOCIOS
    """
    try:
        query = """
        WITH empresas_migradas AS (
            SELECT DISTINCT cnpj_carga_padronizado AS cnpj, data_migracao
            FROM ccee_parcela_carga_consumo_2025
            WHERE data_migracao IS NOT NULL
            AND data_migracao >= (CURRENT_DATE - INTERVAL %s)
        )
        SELECT 
            em.cnpj AS "CNPJ",
            es.nome_fantasia,
            TO_CHAR(em.data_migracao, 'DD-MM-YYYY') AS "DATA_MIGRACAO",
            TO_CHAR(em.data_migracao, 'YYYY-MM') AS "ANO_MES",
            es.uf,
            es.municipio,
            es.cnae_fiscal_principal,
            cn.descricao AS cnae_descricao,
            es.email AS "EMAIL",
            CASE 
                WHEN es.ddd1 IS NOT NULL AND es.telefone1 IS NOT NULL 
                    THEN CONCAT('(', es.ddd1, ') ', es.telefone1)
                WHEN es.ddd1 IS NULL AND es.telefone1 IS NOT NULL 
                    THEN es.telefone1
                ELSE NULL
            END AS "TELEFONE01",
            STRING_AGG(
                CASE WHEN s.nome_socio IS NOT NULL AND s.nome_socio != 'NÃO INFORMADO' 
                    THEN CONCAT(s.nome_socio, ' (CPF/CNPJ: ', 
                               COALESCE(NULLIF(s.cnpj_cpf_socio, ''), 'Não informado'), ')')
                ELSE NULL END, 
                '; '
            ) AS "SOCIOS"
        FROM empresas_migradas em
        JOIN rfb_estabelecimentos es ON em.cnpj = es.cnpj_completo
        LEFT JOIN rfb_socios s ON SUBSTRING(em.cnpj, 1, 8) = s.cnpj_basico
        JOIN aux_rfb_cnaes cn ON es.cnae_fiscal_principal = cn.codigo
        GROUP BY em.cnpj, es.nome_fantasia, em.data_migracao, es.uf, es.municipio, 
                es.cnae_fiscal_principal, cn.descricao, es.email, es.ddd1, es.telefone1
        """
        
        return fetch_data(query, (f"{anos} years",))
        
    except Exception as e:
        logger.error(f"Erro ao consultar empresas migradas: {str(e)}")
        st.error("Erro ao carregar dados das empresas")
        return pd.DataFrame()
    
# =============================================
# CONFIGURAÇÃO DO AGGRID
# =============================================

def configure_aggrid(df, height=400, selection_mode="multiple", use_checkbox=True, enable_search=True):
    """Configura o AgGrid com opções padrão e barra de pesquisa."""
    gb = GridOptionsBuilder.from_dataframe(df)
    
    # Configurações padrão
    gb.configure_default_column(
        filterable=enable_search,
        sortable=True,
        resizable=True,
        groupable=False,
        editable=False,
        wrapText=True,
        autoHeight=True,
        floatingFilter=enable_search
    )
    
    # Configuração de seleção
    gb.configure_selection(
        selection_mode=selection_mode,
        use_checkbox=use_checkbox,
        pre_selected_rows=[],
        header_checkbox=True if use_checkbox else False
    )
    
    # Configuração de paginação
    gb.configure_pagination(
        enabled=True,
        paginationAutoPageSize=False,
        paginationPageSize=20
    )
    
    # Configurações específicas
    if 'Data Migração' in df.columns:
        gb.configure_column('Data Migração', 
                          filter='agDateColumnFilter',
                          floatingFilterComponent='agDateFloatingFilterComponent')
    
    if 'CNPJ' in df.columns:
        gb.configure_column('CNPJ',
                          width=180,
                          filter='agTextColumnFilter',
                          filterParams={
                              'filterOptions': ['contains'],
                              'debounceMs': 300,
                              'suppressAndOrCondition': True
                          },
                          headerTooltip="CNPJ da empresa")
    
    if 'Nome Fantasia' in df.columns:
        gb.configure_column('Nome Fantasia',
                          width=250,
                          filter='agTextColumnFilter',
                          filterParams={
                              'filterOptions': ['contains'],
                              'debounceMs': 300,
                              'suppressAndOrCondition': True
                          },
                          headerTooltip="Nome fantasia da empresa")
    
    grid_options = gb.build()
    
    return AgGrid(
        df,
        gridOptions=grid_options,
        update_mode=GridUpdateMode.SELECTION_CHANGED | GridUpdateMode.FILTERING_CHANGED | GridUpdateMode.SORTING_CHANGED,
        columns_auto_size_mode=ColumnsAutoSizeMode.FIT_CONTENTS,
        height=height,
        theme='streamlit',
        allow_unsafe_jscode=True,
        fit_columns_on_grid_load=False,
        enable_enterprise_modules=True
    )

# =============================================
# COMPONENTES DA INTERFACE
# =============================================
# funcao para filtros
def setup_sidebar_filters(static_data):
    """Configura os filtros na sidebar."""
    st.sidebar.markdown("""
    <div class="d-flex align-items-center mb-3">
        <i class="bi bi-funnel-fill me-2"></i>
        <h5 class="mb-0">Filtros Principais</h5>
    </div>
    """, unsafe_allow_html=True)
    
    anos_analise = st.sidebar.slider(
        "Período de análise (anos)", 
        min_value=1, max_value=10, value=5,
        help="Selecione quantos anos de dados deseja analisar"
    )
    
    visualizacao = st.sidebar.radio(
        "Tipo de visualização", 
        ["Gráfico de Linhas", "Barras Horizontais"],
        format_func=lambda x: f"{'📈' if x == 'Gráfico de Linhas' else '📊'} {x}",
        help="Escolha como visualizar os dados"
    )
    
    porte_filtro = st.sidebar.selectbox(
        "Porte da empresa", 
        ["Todos", "Micro Empresa", "Pequeno Porte", "Médio Porte", "Grande Porte"],
        format_func=lambda x: f"{'🏠' if x == 'Micro Empresa' else '🏢' if x == 'Grande Porte' else '🏛️'} {x}"
    )
    
    uf_filtro = st.sidebar.selectbox(
        "Unidade Federativa (UF)",
        ["Todos"] + static_data["ufs"],
        format_func=lambda x: f"{'🌎' if x == 'Todos' else '📍'} {x}"
    )
    
    return anos_analise, visualizacao, porte_filtro, uf_filtro

#funcao para exibicao dos cards
def show_metrics_cards(anos_analise):
    """Exibe os cards de métricas na interface principal."""
    with st.spinner(f"Atualizando dados para {anos_analise} anos..."):
        df_migracao = query_migracao_por_periodo(anos_analise)

    # Extrai o total de migrações dos últimos 5 anos
    if not df_migracao.empty:
        total_ultimos_anos = df_migracao[df_migracao['ano_mes'] != 'TOTAL']['quantidade_migracoes'].sum()
    else:
        total_ultimos_anos = 0
    
    # Dados para os cards
    agora = get_current_time()
    data_importacao = st.session_state.data_importacao.strftime('%d/%m/%Y') if st.session_state.data_importacao else "N/D"
    total_empresas = st.session_state.total_empresas

       # Renderização dos cards com Bootstrap
    st.markdown(f"""
    <div class="container mt-4">
        <div class="row g-4">
            <div class="col-md-3">
                <div class="card h-100 shadow-sm">
                    <div class="card-body text-center">
                        <i class="bi bi-clock-fill text-primary fs-1 mb-3"></i>
                        <h5 class="card-title">Data Atual</h5>
                        <p class="card-text fs-4 fw-bold">{agora}</p>
                    </div>
                </div>
            </div>
            <div class="col-md-3">
                <div class="card h-100 shadow-sm">
                    <div class="card-body text-center">
                        <i class="bi bi-database-fill text-success fs-1 mb-3"></i>
                        <h5 class="card-title">Última Importação</h5>
                        <p class="card-text fs-4 fw-bold">{data_importacao}</p>
                    </div>
                </div>
            </div>
            <div class="col-md-3">
                <div class="card h-100 shadow-sm">
                    <div class="card-body text-center">
                        <i class="bi bi-building text-warning fs-1 mb-3"></i>
                        <h5 class="card-title">Total Empresas</h5>
                        <p class="card-text fs-4 fw-bold">{format_milhar(total_empresas)}</p>
                    </div>
                </div>
            </div>
            <div class="col-md-3">
                <div class="card h-100 shadow-sm">
                    <div class="card-body text-center">
                        <i class="bi bi-graph-up-arrow text-danger fs-1 mb-3"></i>
                        <h5 class="card-title">Migrações ({anos_analise} anos)</h5>
                        <p class="card-text fs-4 fw-bold">{format_milhar(total_ultimos_anos)}</p>
                    </div>
                </div>
            </div>
        </div>
    </div>
    <div class="alert alert-info mt-3" role="alert">
        <i class="bi bi-info-circle-fill"></i> Dados atualizados constantemente. A atualização depende da CCEE.
    </div>
    """, unsafe_allow_html=True)
    
def show_migration_chart(anos_analise, visualizacao):
    """Exibe o gráfico de evolução das migrações."""
    st.markdown("""
    <div class="d-flex justify-content-between align-items-center mb-4">
        <h2 class="mb-0"><i class="bi bi-graph-up icon-header"></i>Evolução das Migrações</h2>
        <div class="btn-group" role="group">
            <button class="btn btn-outline-primary {'active' if visualizacao == 'Gráfico de Linhas' else ''}" 
                    onclick="parent.document.querySelector('input[type=radio][value=\"Gráfico de Linhas\"]').click()">
                <i class="bi bi-line-chart"></i> Linhas
            </button>
            <button class="btn btn-outline-primary {'active' if visualizacao == 'Barras Horizontais' else ''}" 
                    onclick="parent.document.querySelector('input[type=radio][value=\"Barras Horizontais\"]').click()">
                <i class="bi bi-bar-chart"></i> Barras
            </button>
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    with st.spinner(f"Atualizando dados para {anos_analise} anos..."):
        df_migracao = query_migracao_por_periodo(anos_analise)
    
    if not df_migracao.empty:
        df_grafico = df_migracao[df_migracao['ano_mes'] != 'TOTAL']
        df_grafico['data_formatada'] = pd.to_datetime(df_grafico['ano_mes'], format='%Y-%m')
        df_grafico = df_grafico.sort_values('data_formatada')
        
        col1, col2 = st.columns(2)
        total_migracoes = df_migracao[df_migracao['ano_mes'] == 'TOTAL']['quantidade_migracoes'].values[0]
        col1.metric("Total de Empresas Migradas", format_milhar(total_migracoes))
        
        if not df_grafico.empty:
            ultimo_mes = df_grafico.iloc[-1]
            col2.metric(f"Migrações em {ultimo_mes['ano_mes']}", 
                       format_milhar(ultimo_mes['quantidade_migracoes']))
        
        if visualizacao == "Gráfico de Linhas":
            fig = px.line(
                df_grafico,
                x='ano_mes',
                y='quantidade_migracoes',
                markers=True,
                labels={'ano_mes': 'Período', 'quantidade_migracoes': 'Empresas Migradas'},
                title='Evolução Mensal das Migrações'
            )
            fig.update_layout(
                xaxis={'categoryorder': 'array', 'categoryarray': df_grafico['ano_mes'].tolist()},
                height=500,
                hovermode="x unified"
            )
        else:
            fig = px.bar(
                df_grafico,
                x='quantidade_migracoes',
                y='ano_mes',
                orientation='h',
                labels={'ano_mes': 'Período', 'quantidade_migracoes': 'Empresas Migradas'},
                color='quantidade_migracoes',
                color_continuous_scale='Blues',
                height=600
            )
            fig.update_layout(yaxis={'categoryorder': 'total ascending'})
        
        st.plotly_chart(fig, use_container_width=True)
        
        csv = df_migracao.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="📥 Baixar dados de migração",
            data=csv,
            file_name=f"migracao_mercado_livre_{anos_analise}_anos.csv",
            mime="text/csv"
        )
    else:
        st.warning("Nenhum dado encontrado com os filtros selecionados")

def show_company_details(anos_analise, static_data):
    """Exibe a tabela de detalhes das empresas com opção de enriquecimento."""
    st.markdown("""
    <div class="d-flex justify-content-between align-items-center mb-4">
        <h2 class="mb-0"><i class="bi bi-table icon-header"></i>Detalhes das Empresas Migradas</h2>
        <button class="btn btn-primary btn-lg" type="button" onclick="parent.document.querySelector('.stButton > button').click()">
            <i class="bi bi-cloud-download"></i> Realizar Enriquecimento
        </button>
    </div>
    """, unsafe_allow_html=True)
    
    # Botão para enriquecimento de dados
    if st.button("🔍 Realizar Enriquecimento", key="btn_enriquecimento", 
                help="Executa o script para buscar contatos adicionais"):
        try:
            with st.spinner("Aguarde realizando o enriquecimento dos dados..."):
                # Seleciona apenas CNPJ e Razão Social para enriquecimento
                df_enriquecimento = st.session_state.df_empresas[['CNPJ', 'nome_fantasia']].copy()
                
                # Converte para lista de dicionários
                empresas_para_enriquecer = df_enriquecimento.to_dict('records')
                
                # Chama a função de enriquecimento
                resultado = processar_enriquecimento(empresas_para_enriquecer)
                
                if resultado['sucesso']:
                    st.success("Enriquecimento realizado com sucesso!")
                    st.info(f"{resultado['quantidade']} empresas enriquecidas")
                    
                    # Atualiza a tabela no banco de dados
                    atualizar_banco_dados(resultado['dados'])
                else:
                    st.error("Erro ao executar enriquecimento:")
                    st.error(resultado['erro'])
        except Exception as e:
            st.error(f"Falha ao executar enriquecimento: {str(e)}")
    
    # Filtros avançados
    with st.expander("🔍 Filtros Avançados", expanded=False):
        col1, col2, col3 = st.columns(3)
        with col1:
            uf_filtro = st.selectbox(
                "Filtrar por UF", 
                ["Todos"] + static_data["ufs"],
                key="uf_filtro_avancado"
            )
        with col2:
            cnae_filtro = st.selectbox(
                "Filtrar por CNAE", 
                st.session_state.cnae_options,
                key="cnae_filtro"
            )
        with col3:
            data_inicio, data_fim = st.date_input(
                "Período de migração",
                value=[datetime.now() - timedelta(days=365*anos_analise), datetime.now()],
                max_value=datetime.now()
            )
    
    try:
        with st.spinner("Aplicando filtros..."):
            df_empresas = st.session_state.df_empresas.copy()
            
            if not df_empresas.empty:
                # Aplicar filtros
                if uf_filtro != "Todos":
                    df_empresas = df_empresas[df_empresas['uf'] == uf_filtro]
                
                if cnae_filtro != "Todos":
                    cnae_codigo = cnae_filtro.split(" - ")[0]
                    df_empresas = df_empresas[df_empresas['cnae_fiscal_principal'] == cnae_codigo]
                
                df_empresas['data_migracao'] = pd.to_datetime(df_empresas['DATA_MIGRACAO'], format='%d-%m-%Y').dt.date
                data_inicio = pd.to_datetime(data_inicio).date()
                data_fim = pd.to_datetime(data_fim).date()
                df_empresas = df_empresas[
                    (df_empresas['data_migracao'] >= data_inicio) & 
                    (df_empresas['data_migracao'] <= data_fim)
                ]
                
                df_empresas['CNPJ'] = df_empresas['CNPJ'].apply(format_cnpj)
                
                # Colunas para exibição (incluindo Razão Social)
                cols_to_show = {
                    'CNPJ': 'CNPJ',
                    'nome_fantasia': 'Razão Social',
                    'DATA_MIGRACAO': 'Data Migração',
                    'ANO_MES': 'Ano/Mês',
                    'uf': 'UF',
                    'SOCIOS': 'Sócios',
                    'EMAIL': 'E-mail',
                    'TELEFONE01': 'Telefone 1'
                }
                
                present_cols = [col for col in cols_to_show.keys() if col in df_empresas.columns]
                df_display = df_empresas[present_cols].rename(columns={k: v for k, v in cols_to_show.items() if k in present_cols})
                
                st.success(f"{len(df_display)} empresas encontradas.")
                
                # Configuração da tabela AgGrid
                grid_response = configure_aggrid(
                    df_display,
                    height=600,
                    enable_search=True,
                    use_checkbox=False
                )
                
                # Botão de download
                csv_detalhes = df_display.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="📥 Baixar detalhes completos",
                    data=csv_detalhes,
                    file_name=f"detalhes_empresas_migracao_ml_{anos_analise}_anos.csv",
                    mime="text/csv",
                    key="download_detalhes"
                )
            else:
                st.warning("Nenhuma empresa encontrada com os critérios selecionados.")
    except Exception as e:
        st.error(f"Erro ao consultar dados: {str(e)}")

def processar_enriquecimento(empresas: list) -> dict:
    """
    Processa o enriquecimento de dados para uma lista de empresas.
    
    Args:
        empresas: Lista de dicionários com CNPJ e Razão Social
        
    Returns:
        Dicionário com:
        - sucesso: bool indicando sucesso da operação
        - quantidade: número de empresas processadas
        - dados: lista de dicionários com dados enriquecidos
        - erro: mensagem de erro (se houver)
    """
    resultados = []
    
    try:
        for empresa in empresas:
            cnpj = empresa['CNPJ']
            razao_social = empresa['nome_fantasia']
            
            # Aqui você implementaria a lógica de scraping para cada empresa
            # Exemplo fictício:
            dados_enriquecidos = {
                'cnpj': cnpj,
                'razao_social': razao_social,
                'telefones': ['+5511999999999'],
                'emails': ['contato@empresa.com'],
                'redes_sociais': {
                    'linkedin': 'https://linkedin.com/company/empresa'
                },
                'data_processamento': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
            resultados.append(dados_enriquecidos)
        
        return {
            'sucesso': True,
            'quantidade': len(resultados),
            'dados': resultados,
            'erro': None
        }
        
    except Exception as e:
        return {
            'sucesso': False,
            'quantidade': 0,
            'dados': [],
            'erro': str(e)
        }
def criar_tabela_dados():
    """Cria a tabela 'dados' no banco de dados se não existir."""
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS dados (
        id SERIAL PRIMARY KEY,
        cnpj VARCHAR(14) NOT NULL,
        razao_social VARCHAR(255),
        telefones JSONB,
        emails JSONB,
        redes_sociais JSONB,
        endereco TEXT,
        data_processamento TIMESTAMP,
        data_atualizacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        CONSTRAINT cnpj_unique UNIQUE (cnpj)
    );
    CREATE INDEX IF NOT EXISTS idx_dados_cnpj ON dados(cnpj);
    """
    
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(create_table_sql)
                conn.commit()
        logger.info("Tabela 'dados' criada/verificada com sucesso")
    except Exception as e:
        logger.error(f"Erro ao criar tabela 'dados': {str(e)}")
        raise

def atualizar_banco_dados(dados: list):
    """Atualiza o banco de dados com os dados enriquecidos."""
    criar_tabela_dados()  # Garante que a tabela existe
    
    insert_sql = """
    INSERT INTO dados (cnpj, razao_social, telefones, emails, redes_sociais, endereco, data_processamento)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (cnpj) DO UPDATE
    SET
        razao_social = EXCLUDED.razao_social,
        telefones = EXCLUDED.telefones,
        emails = EXCLUDED.emails,
        redes_sociais = EXCLUDED.redes_sociais,
        endereco = EXCLUDED.endereco,
        data_processamento = EXCLUDED.data_processamento,
        data_atualizacao = CURRENT_TIMESTAMP
    """
    
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                for item in dados:
                    cursor.execute(insert_sql, (
                        item['cnpj'],
                        item['razao_social'],
                        json.dumps(item.get('telefones', [])),
                        json.dumps(item.get('emails', [])),
                        json.dumps(item.get('redes_sociais', {})),
                        item.get('endereco', ''),
                        item.get('data_processamento')
                    ))
                conn.commit()
        logger.info(f"Dados de {len(dados)} empresas atualizados no banco")
    except Exception as e:
        logger.error(f"Erro ao atualizar banco de dados: {str(e)}")
        raise
            
def show_footer():
    """Exibe o rodapé do dashboard."""
    st.markdown("""
    <footer class="footer mt-5 py-3 bg-light">
        <div class="container">
            <div class="row">
                <div class="col-md-6">
                    <h5><i class="bi bi-database"></i> Fontes de Dados</h5>
                    <ul class="list-unstyled">
                        <li><a href="https://www.gov.br/receitafederal/" target="_blank" class="text-decoration-none">
                            <i class="bi bi-box-arrow-up-right"></i> RFB - Receita Federal do Brasil</a></li>
                        <li><a href="https://dadosabertos.ccee.org.br/dataset/parcela_carga_consumo/resource/c88d04a6-fe42-413b-b7bf-86e390494fb0" 
                               target="_blank" class="text-decoration-none">
                            <i class="bi bi-box-arrow-up-right"></i> CCEE - Dados Abertos</a></li>
                    </ul>
                </div>
                <div class="col-md-6 text-end">
                    <p><i class="bi bi-clock"></i> Última atualização: {get_current_time()}</p>
                    <p><i class="bi bi-speedometer2"></i> Tempo de processamento: {(time.time() - tempo_inicio):.2f} segundos</p>
                </div>
            </div>
            <hr>
            <div class="text-center text-muted">
                <p>Desenvolvido por Ariel Rosa da Luz | Julho 2025 | Versão 3.0</p>
            </div>
        </div>
    </footer>
    """, unsafe_allow_html=True)

# =============================================
# EXECUÇÃO PRINCIPAL
# =============================================

def main():
    """Função principal do aplicativo."""
    initialize_session_state()
    
    # Garante que a tabela de dados existe
    try:
        criar_tabela_dados()
    except Exception as e:
        st.error(f"Falha ao verificar tabela de dados: {str(e)}")
        return
    
    # Inicialização do estado da sessão
    if 'dados_carregados' not in st.session_state:
        st.session_state.update({
            'dados_carregados': False,
            'carregamento_iniciado': False,
            'static_data': None,
            'df_migracao': None,
            'df_empresas': None,
            'data_importacao': None,
            'total_empresas': None,
            'cnae_options': []
        })

    # Carrega ícones Bootstrap
    st.markdown(
        '<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap-icons@1.11.3/font/bootstrap-icons.min.css">',
        unsafe_allow_html=True
    )

    # Configuração do estilo (adicionar no início do código)
    st.markdown("""
        <style>
            /* Aumenta o tamanho da fonte geral */
            html, body, .stApp, .stMarkdown, .stText {
                font-size: 18px !important;
            }
            
            /* Títulos maiores */
            h1 {
                font-size: 32px !important;
                color: #2c3e50;
                margin-bottom: 20px;
            }
            
            h2 {
                font-size: 26px !important;
                color: #34495e;
                border-bottom: 2px solid #3498db;
                padding-bottom: 5px;
            }
            
            h3 {
                font-size: 22px !important;
            }
            
            /* Descrição destacada */
            .description {
                font-size: 22px;
                line-height: 1.6;
                color: white;
                background-color: gray;
                padding: 20px;
                border-radius: 10px;
                margin-bottom: 30px;
                border-left: 5px solid gray;
            }
        </style>
        """, unsafe_allow_html=True
        )

    # Título e descrição (substituir a seção existente)
    st.title("⚡ Painel Estratégico: Migração para o Mercado Livre de Energia")

    st.markdown("""
        <div class="description">
            Este dashboard monitora a transição de empresas para o Mercado Livre de Energia,
            combinando dados cadastrais da Receita Federal com informações de migração da CCEE.<br>
            Permite identificar tendências, analisar perfis corporativos e extrair relatórios
            estratégicos para o setor energético.
        </div>
    """, unsafe_allow_html=True)

    # Carregamento inicial dos dados
     # Carrega dados estáticos (sem UI)
    if not st.session_state.dados_carregados:
        if not st.session_state.carregamento_iniciado:
            st.session_state.carregamento_iniciado = True
            
            with st.spinner("🔍 Carregando dados iniciais..."):
                try:
                    # Carrega dados estáticos brutos
                    static_data_raw = get_static_data()
                    
                    # Prepara opções de CNAE (isso NÃO deve estar na função cacheada)
                    cnaes_df = static_data_raw["cnaes"]
                    cnaes_df['descricao_resumida'] = cnaes_df['descricao'].str.slice(0, 40) + '...'
                    cnae_options = ["Todos"] + [
                        f"{row['codigo']} - {row['descricao_resumida']}" 
                        for _, row in cnaes_df.iterrows()
                    ]
                    
                    # Atualiza o estado da sessão
                    st.session_state.update({
                        'static_data': static_data_raw,
                        'cnae_options': cnae_options,
                        'df_migracao': query_migracao_por_periodo(5),
                        'df_empresas': query_empresas_migradas(5),
                        'data_importacao': fetch_data(
                            "SELECT MAX(data_importacao) FROM ccee_parcela_carga_consumo_2025"
                        ).iloc[0][0],
                        'total_empresas': fetch_data(
                            "SELECT COUNT(DISTINCT cnpj_carga) FROM ccee_parcela_carga_consumo_2025 WHERE data_migracao IS NOT NULL"
                        ).iloc[0][0],
                        'dados_carregados': True
                    })
                    
                    st.toast("✅ Dados carregados com sucesso!", icon="✅")
                except Exception as e:
                    st.error(f"Falha ao carregar dados: {str(e)}")
                    st.session_state.carregamento_iniciado = False
                    st.stop()
        else:
            st.warning("⌛ Aguarde, carregando dados...")
            st.stop()

    # Configura filtros na sidebar
    anos_analise, visualizacao, porte_filtro, uf_filtro = setup_sidebar_filters(st.session_state.static_data)

    # Exibe componentes principais
    show_metrics_cards(anos_analise)
    show_migration_chart(anos_analise, visualizacao)
    show_company_details(anos_analise, st.session_state.static_data)
    show_footer()

if __name__ == "__main__":
    main()