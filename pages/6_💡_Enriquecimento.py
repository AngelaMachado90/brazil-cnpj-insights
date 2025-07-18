"""
Dashboard de Enriquecimento de Dados RFB + CCEE

Este aplicativo fornece uma análise estratégica das empresas que realizaram a transição
para o Mercado Livre de Energia, com dados integrados da Receita Federal e CCEE.

Funcionalidades principais:
- Análise temporal da migração (últimos 1-10 anos)
- Filtros por localização (UF/município), setor (CNAE) e porte
- Detalhamento completo por empresa (CNPJ, contatos, sócios)
- Visualização gráfica e exportação de dados
- Monitoramento em tempo real da base de dados

Requisitos:
- Python 3.7+
- Conexão PostgreSQL com dados RFB+CCEE
- Bibliotecas listadas em requirements.txt

Desenvolvido por:
Angela Machado | Julho 2025 | Versão 2.1
"""

import streamlit as st
import pandas as pd
import psycopg2
import plotly.express as px
from datetime import datetime, timedelta
import pytz
import time
from contextlib import contextmanager
from functools import wraps
import logging
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, ColumnsAutoSizeMode

# =============================================
# CONFIGURAÇÃO INICIAL
# =============================================

# Configuração básica de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuração do Streamlit
st.set_page_config(
    page_title="Dashboard Migração Mercado Livre",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded"
)

# =============================================
# CONSTANTES DE CONFIGURAÇÃO
# =============================================

class Config:
    """Classe para armazenar configurações constantes do aplicativo"""
    
    # Configuração de timezone
    TIMEZONE = "America/Sao_Paulo"
    
    # Configuração do banco de dados
    DB_CONFIG = {
        "host": "emewe-mailling-db",
        "database": "cnpj_receita",
        "user": "postgres",
        "password": "postgres",
        "port": 5432
    }

# Início da contagem de tempo para monitoramento de performance
tempo_inicio = time.time()

# =============================================
# FUNÇÕES UTILITÁRIAS
# =============================================

def format_milhar(n: int) -> str:
    """Formata número com separador de milhar."""
    return f"{n:,.0f}".replace(",", ".")

def format_cnpj(cnpj: str) -> str:
    """Formata CNPJ com máscara padrão (XX.XXX.XXX/XXXX-XX)."""
    cnpj = str(cnpj).zfill(14)
    return f"{cnpj[:2]}.{cnpj[2:5]}.{cnpj[5:8]}/{cnpj[8:12]}-{cnpj[12:14]}"

def get_current_time() -> str:
    """Retorna a data/hora atual formatada conforme timezone configurado."""
    fuso = pytz.timezone(Config.TIMEZONE)
    return datetime.now(fuso).strftime("%d/%m/%Y %H:%M:%S")

@contextmanager
def get_db_connection():
    """Gerenciador de contexto para conexões com o banco de dados."""
    conn = None
    try:
        conn = psycopg2.connect(**Config.DB_CONFIG)
        conn.set_client_encoding('UTF8')
        yield conn
    finally:
        if conn is not None:
            conn.close()

def timing_decorator(func):
    """Decorador para medir e exibir tempo de execução de funções."""
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

# =============================================
# FUNÇÕES DE ACESSO A DADOS
# =============================================

@st.cache_data(ttl=600, show_spinner="Carregando dados...")
def fetch_data(query: str) -> pd.DataFrame:
    """Executa consulta SQL e retorna DataFrame."""
    with get_db_connection() as conn:
        try:
            df = pd.read_sql(query, conn)
            return df
        except Exception as e:
            st.error(f"Erro ao executar consulta: {e}")
            return pd.DataFrame()

@st.cache_data(ttl=3600)
@timing_decorator
def get_static_data() -> dict:
    """Carrega dados estáticos para filtros."""
    data = {
        "ufs": [],
        "municipios": pd.DataFrame(),
        "cnaes": pd.DataFrame(),
        "anos": []
    }
    try:
        data["ufs"] = fetch_data(
            "SELECT DISTINCT uf FROM rfb_estabelecimentos ORDER BY uf"
        )["uf"].dropna().tolist()
        data["municipios"] = fetch_data(
            "SELECT * FROM vw_municipios_com_estabelecimentos ORDER BY descricao"
        )
        data["cnaes"] = fetch_data(
            "SELECT DISTINCT c.codigo, c.descricao FROM cnae_10 c ORDER BY c.codigo"
        )
        anos_df = fetch_data("""
            SELECT DISTINCT EXTRACT(YEAR FROM data_situacao_cadastral) AS ano
            FROM vw_estabelecimentos_empresas
            WHERE data_situacao_cadastral IS NOT NULL
            ORDER BY ano DESC
        """)
        if not anos_df.empty:
            data["anos"] = anos_df['ano'].dropna().astype(int).tolist()
    except Exception as e:
        st.error(f"Erro ao carregar dados estáticos: {e}")
    return data

@st.cache_data(ttl=3600)
@timing_decorator
def query_empresas_migradas(anos=5):
    """Consulta empresas que migraram para o mercado livre."""
    try:
        with get_db_connection() as conn:
            query = f"""
            WITH empresas_migradas AS (
                SELECT DISTINCT c.cnpj_carga_padronizado AS cnpj, c.data_migracao
                FROM ccee_parcela_carga_consumo_2025 c
                WHERE c.data_migracao IS NOT NULL
                AND c.data_migracao >= (CURRENT_DATE - INTERVAL '{anos} years')
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
                    WHEN es.ddd1 IS NOT NULL AND es.telefone1 IS NOT NULL THEN CONCAT('(', es.ddd1, ') ', es.telefone1)
                    WHEN es.ddd1 IS NULL AND es.telefone1 IS NOT NULL THEN es.telefone1
                    ELSE NULL
                END AS "TELEFONE01",
                STRING_AGG(
                    CASE WHEN s.nome_socio IS NOT NULL AND s.nome_socio != 'NÃO INFORMADO' THEN 
                        CONCAT(s.nome_socio, ' (CPF/CNPJ: ', COALESCE(NULLIF(s.cnpj_cpf_socio, ''), 'Não informado'), ')')
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
            df = pd.read_sql(query, conn)
            return df
    except Exception as e:
        st.error(f"Erro ao consultar empresas migradas: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=3600)
@timing_decorator
def query_migracao_por_periodo(anos=5):
    """Consulta migrações por período."""
    try:
        with get_db_connection() as conn:
            query = f"""
            WITH dados_migracao AS (
                SELECT 
                    TO_CHAR(data_migracao, 'YYYY-MM') AS ano_mes,
                    COUNT(DISTINCT cnpj_carga) AS quantidade_migracoes,
                    0 AS eh_total
                FROM ccee_parcela_carga_consumo_2025
                WHERE data_migracao IS NOT NULL
                AND data_migracao >= (CURRENT_DATE - INTERVAL '{anos} years')
                GROUP BY TO_CHAR(data_migracao, 'YYYY-MM')
                UNION ALL
                SELECT 'TOTAL' AS ano_mes,
                    COUNT(DISTINCT cnpj_carga) AS quantidade_migracoes,
                    1 AS eh_total
                FROM ccee_parcela_carga_consumo_2025
                WHERE data_migracao IS NOT NULL
                AND data_migracao >= (CURRENT_DATE - INTERVAL '{anos} years')
            )
            SELECT ano_mes, quantidade_migracoes
            FROM dados_migracao
            ORDER BY eh_total, ano_mes;
            """
            df = pd.read_sql(query, conn)
            return df
    except Exception as e:
        st.error(f"Erro ao consultar migrações por período: {e}")
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

def setup_sidebar_filters(static_data):
    """Configura os filtros na sidebar."""
    st.sidebar.header("Filtros Principais")
    
    anos_analise = st.sidebar.slider(
        "Período de análise (anos)", 
        min_value=1, max_value=10, value=5,
        help="Selecione quantos anos de dados deseja analisar"
    )
    
    visualizacao = st.sidebar.radio(
        "Tipo de visualização", 
        ["Gráfico de Linhas", "Barras Horizontais"],
        help="Escolha como visualizar os dados"
    )
    
    porte_filtro = st.sidebar.selectbox(
        "Porte da empresa", 
        ["Todos", "Micro Empresa", "Pequeno Porte", "Médio Porte", "Grande Porte"]
    )
    
    uf_filtro = st.sidebar.selectbox(
        "Unidade Federativa (UF)",
        ["Todos"] + static_data["ufs"]
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

    # CSS customizado para os cards
    st.markdown("""
    <style>
        .card-container {
            display: flex;
            flex-wrap: wrap;
            gap: 20px;
            margin: 20px 0;
            justify-content: space-between;
        }
        .card {
            width: 23%;
            min-width: 200px;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white !important;
            border-radius: 15px;
            padding: 20px;
            box-shadow: 0 4px 12px rgba(0,0,0,0.3);
            text-align: center;
            font-weight: bold;
        }
        .card-icon {
            font-size: 40px;
            margin-bottom: 10px;
            color: white !important;
        }
        .card-label {
            font-size: 18px;
            color: #D1D5DB;
            margin-bottom: 5px;
        }
        .card-value {
            font-size: 32px;
            color: white;
        }
        .text-explanation {
            font-size: 20px;
            line-height: 1.4;
            margin-top: 8px;
            padding: 6px 12px;
            border-radius: 8px;
            display: inline-block;
            box-shadow: 0 1px 2px rgba(0,0,0,0.1);
            font-style: italic;
        }
    
        /* Versão alternativa com ícone */
        .text-explanation.with-icon {
            padding-left: 30px;
            position: relative;
        }
            
        .text-explanation.with-icon:before {
            content: "ℹ️";
            position: absolute;
            left: 8px;
            top: 50%;
            transform: translateY(-50%);
        }
            
        /* Versão de destaque */
        .text-explanation.highlight {
            color: #1976d2;
            border-left: 4px solid #1976d2;
        }
            
        /* Versão minimalista */
        .text-explanation.minimal {
            background: transparent;
            color: #666;
            padding: 2px 0;
            box-shadow: none;
            font-style: normal;
        }

        @media (max-width: 1200px) {
            .card {
                width: 48%;
            }
        }
        @media (max-width: 768px) {
            .card {
                width: 100%;
            }
        }
    </style>
    """, unsafe_allow_html=True)

    # Carrega ícones Bootstrap
    st.markdown(
        '<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap-icons@1.11.3/font/bootstrap-icons.min.css">',
        unsafe_allow_html=True
    )

    # Dados para os cards
    agora = get_current_time()
    data_importacao = st.session_state.data_importacao.strftime('%d/%m/%Y') if st.session_state.data_importacao else "N/D"
    total_empresas = st.session_state.total_empresas


    # Renderização dos cards
    st.markdown(f"""
    <div class="card-container">
        <div class="card">
            <div class="card-icon"><i class="bi bi-clock"></i></div>
            <div class="card-label">Data Atual</div>
            <div class="card-value">{agora}</div>
        </div>
        <div class="card">
            <div class="card-icon"><i class="bi bi-database"></i></div>
            <div class="card-label">Última Importação*</div>
            <div class="card-value">{data_importacao}</div>
        </div>
        <div class="card">
            <div class="card-icon"><i class="bi bi-building"></i></div>
            <div class="card-label">Total Empresas Migradas</div>
            <div class="card-value">{format_milhar(total_empresas)}</div>
        </div>
        <div class="card">
            <div class="card-icon"><i class="bi bi-bar-chart-line"></i></div>
            <div class="card-label">Migradas nos últimos anos**</div>
            <div class="card-value">{format_milhar(total_ultimos_anos)}</div>
        </div>
        <div class="text-explanation">
            <p> *A atualização depende da CCEE.<br>
                Dados constantemente monitorados.</p
            <p> **Empresas migradas para o Mercado Livre nos últimos {anos_analise} anos.</p>
            <p>
        </div>
    </div>
    """, unsafe_allow_html=True)

def show_migration_chart(anos_analise, visualizacao):
    """Exibe o gráfico de evolução das migrações."""
    st.subheader("📈 Evolução das Migrações")
    
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
    """Exibe a tabela de detalhes das empresas."""
    st.subheader("📋 Detalhes das Empresas Migradas")
    
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
                
                cols_to_show = {
                    'CNPJ': 'CNPJ',
                    'nome_fantasia': 'Nome Fantasia',
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

def show_footer():
    """Exibe o rodapé do dashboard."""
    st.markdown("---")
    st.markdown(f"""
    <div style="text-align: center; color: gray;">
        <strong> Fonte de Dados</strong><br>
        <br>
        <ul style="list-style:none; padding:0;">
             <li><a href="https://www.gov.br/receitafederal/" target="_blank">RFB - Receita Federal do Brasil</a></li>
             <li><a href="https://dadosabertos.ccee.org.br/dataset/parcela_carga_consumo/resource/c88d04a6-fe42-413b-b7bf-86e390494fb0" target="_blank">CCEE - Dados Aberto</a></li>
        </ul>
        <p>Última atualização: {get_current_time()}</p>
        <p>Tempo total de processamento: {(time.time() - tempo_inicio):.2f} segundos</p>
    </div>
    """, unsafe_allow_html=True)

# =============================================
# EXECUÇÃO PRINCIPAL
# =============================================

def main():
    """Função principal do aplicativo."""
    
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
    if not st.session_state.dados_carregados:
        if not st.session_state.carregamento_iniciado:
            st.session_state.carregamento_iniciado = True
            
            with st.spinner("🔍 Carregando dados iniciais..."):
                # Carrega dados estáticos
                st.session_state.static_data = get_static_data()
                
                # Prepara opções de CNAE
                cnaes_df = st.session_state.static_data["cnaes"]
                cnaes_df['descricao_resumida'] = cnaes_df['descricao'].str.slice(0, 40) + '...'
                st.session_state.cnae_options = ["Todos"] + [
                    f"{row['codigo']} - {row['descricao_resumida']}" 
                    for _, row in cnaes_df.iterrows()
                ]
                
                # Carrega dados de migração
                st.session_state.df_migracao = query_migracao_por_periodo(5)
                st.session_state.df_empresas = query_empresas_migradas(5)
                
                # Carrega metadados
                data_importacao_df = fetch_data(
                    "SELECT MAX(data_importacao) FROM ccee_parcela_carga_consumo_2025"
                )
                st.session_state.data_importacao = data_importacao_df.iloc[0][0] if not data_importacao_df.empty else None
                
                total_empresas_df = fetch_data(
                    "SELECT COUNT(DISTINCT cnpj_carga) FROM ccee_parcela_carga_consumo_2025 WHERE data_migracao IS NOT NULL"
                )
                st.session_state.total_empresas = total_empresas_df.iloc[0][0] if not total_empresas_df.empty else 0
                
                st.session_state.dados_carregados = True
                st.toast("✅ Dados carregados com sucesso!", icon="✅")
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