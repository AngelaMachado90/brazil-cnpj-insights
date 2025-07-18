"""
Dashboard de Estabelecimentos CNPJ

Este aplicativo Streamlit fornece uma interface interativa para análise de dados de estabelecimentos
cadastrados na Receita Federal do Brasil, permitindo filtragem avançada, visualização gráfica e
exportação de dados.

Funcionalidades principais:
- Filtros por UF, município, CNAE, porte e situação cadastral
- Visualização em gráficos e tabelas interativas
- Exportação de resultados para CSV
- Monitoramento de performance

Requisitos:
- Python 3.7+
- Conexão com banco PostgreSQL contendo os dados da RFB

Desenvolvido:
Nome: Angela Machado.
Data: Julho de 2025.
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
    page_title="Dashboard CNPJ",
    page_icon="📊",
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
    """
    Formata número com separador de milhar.
    
    Args:
        n (int): Número a ser formatado
        
    Returns:
        str: Número formatado com separadores (ex: 1.000)
    """
    return f"{n:,.0f}".replace(",", ".")

def format_cnpj(cnpj: str) -> str:
    """
    Formata CNPJ com máscara padrão (XX.XXX.XXX/XXXX-XX).
    
    Args:
        cnpj (str): CNPJ sem formatação (14 dígitos)
        
    Returns:
        str: CNPJ formatado
    """
    cnpj = str(cnpj).zfill(14)
    return f"{cnpj[:2]}.{cnpj[2:5]}.{cnpj[5:8]}/{cnpj[8:12]}-{cnpj[12:14]}"

def get_current_time() -> str:
    """
    Retorna a data/hora atual formatada conforme timezone configurado.
    
    Returns:
        str: Data/hora no formato DD/MM/YYYY HH:MM:SS
    """
    fuso = pytz.timezone(Config.TIMEZONE)
    return datetime.now(fuso).strftime("%d/%m/%Y %H:%M:%S")

@contextmanager
def db_connection():
    """
    Gerenciador de contexto para conexões com o banco de dados.
    
    Yields:
        connection: Conexão com o banco de dados
        
    Raises:
        Exception: Erros de conexão são capturados e exibidos via Streamlit
    """
    conn = None
    try:
        conn = psycopg2.connect(**Config.DB_CONFIG)
        yield conn
    except Exception as e:
        st.error(f"Erro de conexão: {str(e)}")
        raise
    finally:
        if conn:
            conn.close()

def timing_decorator(func):
    """
    Decorador para medir e exibir tempo de execução de funções.
    
    Args:
        func: Função a ser decorada
        
    Returns:
        function: Função wrapper com medição de tempo
    """
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
    """
    Executa consulta SQL e retorna DataFrame com gerenciamento adequado de conexão.
    
    Args:
        query (str): Consulta SQL a ser executada
        
    Returns:
        pd.DataFrame: Resultado da consulta em formato DataFrame
        
    Raises:
        Exception: Erros durante a execução da consulta são exibidos via Streamlit
    """
    conn = None
    try:
        conn = psycopg2.connect(**Config.DB_CONFIG)
        return pd.read_sql(query, conn)
    except Exception as e:
        st.error(f"Erro ao executar consulta: {e}")
        return pd.DataFrame()
    finally:
        if conn:
            conn.close()

@timing_decorator
def get_static_data() -> dict:
    """
    Carrega dados estáticos para filtros com gerenciamento de conexão por consulta.
    
    Returns:
        dict: Dicionário contendo:
            - ufs: Lista de unidades federativas
            - municipios: DataFrame com códigos e nomes de municípios
            - cnaes: DataFrame com códigos e descrições de CNAEs
            - portes: Lista de portes de empresas
            - anos: Lista de anos disponíveis
    """
    data = {
        "ufs": [],
        "municipios": pd.DataFrame(),
        "cnaes": pd.DataFrame(),
        "portes": [],
        "anos": []
    }
    
    try:
        # Consulta para UFs
        data["ufs"] = fetch_data(
            "SELECT DISTINCT uf FROM rfb_estabelecimentos ORDER BY uf"
        )["uf"].dropna().tolist()
        
        # Consulta para municípios
        data["municipios"] = fetch_data(
            "SELECT * FROM vw_municipios_com_estabelecimentos ORDER BY descricao"
        )
        
        # Consulta para CNAEs
        data["cnaes"] = fetch_data(
            "SELECT DISTINCT c.codigo, c.descricao FROM cnae_10 c ORDER BY c.codigo"
        )
        
        # Consulta para portes
        data["portes"] = fetch_data(
            "SELECT DISTINCT porte FROM empresas ORDER BY porte"
        )["porte"].dropna().tolist()
        
        # Consulta para anos
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

# =============================================
# INTERFACE DO USUÁRIO
# =============================================

def setup_sidebar_filters(static_data: dict) -> str:
    """
    Configura os filtros na sidebar e retorna a cláusula WHERE para consultas SQL.
    
    Args:
        static_data (dict): Dados estáticos carregados para os filtros
        
    Returns:
        str: Cláusula WHERE construída com base nos filtros selecionados
    """
    st.sidebar.header("Filtros Gerais")
    
    # Filtros principais
    uf_filtro = st.sidebar.selectbox("UF", options=["Todos"] + static_data["ufs"])
    cidade_filtro = st.sidebar.selectbox("Cidade", options=["Todos"] + static_data["municipios"]["descricao"].tolist())

    # Filtro CNAE formatado
    cnae_options = ["Todos"] + [f"{row['codigo']} - {row['descricao']}" for _, row in static_data["cnaes"].iterrows()]
    cnae_filtro = st.sidebar.selectbox("CNAE", options=cnae_options)

    # Filtros avançados
    st.sidebar.header("Filtros Avançados")
    ano_filtro = st.sidebar.selectbox("Ano da Situação Cadastral", options=["Todos"] + [str(a) for a in static_data["anos"]])
    porte_filtro = st.sidebar.selectbox("Porte da Empresa", options=["Todos"] + static_data["portes"])
    simples_filtro = st.sidebar.radio("Optante pelo Simples?", options=["Todos", "Sim", "Não"], index=0)
    mei_filtro = st.sidebar.radio("Optante pelo MEI?", options=["Todos", "Sim", "Não"], index=0)

    # Construção dos filtros SQL
    where_clauses = []

    # Filtros básicos
    if uf_filtro != "Todos":
        where_clauses.append(f"e.uf = '{uf_filtro}'")

    if cidade_filtro != "Todos":
        cidade_codigo = static_data["municipios"].loc[static_data["municipios"]['descricao'] == cidade_filtro, 'municipio'].values[0]
        where_clauses.append(f"e.municipio = '{cidade_codigo}'")

    if cnae_filtro != "Todos":
        cnae_codigo = cnae_filtro.split(" - ")[0]
        where_clauses.append(f"e.cnae_fiscal_principal = '{cnae_codigo}'")

    # Filtros avançados
    if ano_filtro != "Todos":
        where_clauses.append(f"EXTRACT(YEAR FROM est.data_situacao_cadastral) = {ano_filtro}")

    if porte_filtro != "Todos":
        where_clauses.append(f"emp.porte = '{porte_filtro}'")

    if simples_filtro != "Todos":
        val_simples = 'S' if simples_filtro == "Sim" else 'N'
        where_clauses.append(f"emp.opcao_simples = '{val_simples}'")

    if mei_filtro != "Todos":
        val_mei = 'S' if mei_filtro == "Sim" else 'N'
        where_clauses.append(f"emp.opcao_mei = '{val_mei}'")

    return "WHERE " + " AND ".join(where_clauses) if where_clauses else ""

def show_metrics_cards(filtros_sql: str):
    """
    Exibe os cards de métricas na interface principal.
    
    Args:
        filtros_sql (str): Cláusula WHERE com filtros aplicados
    """
    # Consultas para os cards
    query_total_municipios = f"""
        SELECT COUNT(DISTINCT municipio) AS total_municipios
        FROM vw_estabelecimentos_completo
        {filtros_sql.replace('e.', '').replace('emp.', '')}
    """

    query_total_cnaes = f"""
        SELECT COUNT(DISTINCT cnae_fiscal_principal) AS total_cnaes 
        FROM vw_estabelecimentos_completo 
        {filtros_sql.replace('e.', '').replace('emp.', '')}
    """

    query_total_estabelecimentos = f"""
        SELECT COUNT(*) AS total_estabelecimentos 
        FROM vw_estabelecimentos_completo
        {filtros_sql.replace('e.', '').replace('emp.', '')}
    """

    # Buscar dados para os cards
    try:
        total_municipios = fetch_data(query_total_municipios).iloc[0]['total_municipios']
        total_cnaes = fetch_data(query_total_cnaes).iloc[0]['total_cnaes']
        total_estabelecimentos = fetch_data(query_total_estabelecimentos).iloc[0]['total_estabelecimentos']
        agora = get_current_time()
        
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
        </style>
        """, unsafe_allow_html=True)

        # Renderização dos cards
        st.markdown(f"""
        <div class="card-container">
            <div class="card">
                <div class="card-icon"><i class="bi bi-clock"></i></div>
                <div class="card-label">Última Atualização</div>
                <div class="card-value">{agora}</div>
            </div>
            <div class="card">
                <div class="card-icon"><i class="bi bi-building"></i></div>
                <div class="card-label">Municípios</div>
                <div class="card-value">{format_milhar(total_municipios)}</div>
            </div>
            <div class="card">
                <div class="card-icon"><i class="bi bi-list-task"></i></div>
                <div class="card-label">CNAEs</div>
                <div class="card-value">{format_milhar(total_cnaes)}</div>
            </div>
            <div class="card">
                <div class="card-icon"><i class="bi bi-people"></i></div>
                <div class="card-label">Estabelecimentos</div>
                <div class="card-value">{format_milhar(total_estabelecimentos)}</div>
            </div>
        </div>
        """, unsafe_allow_html=True)
    except Exception as e:
        st.error(f"Erro ao carregar métricas: {e}")

def show_top_cities_chart(filtros_sql: str):
    """
    Exibe o gráfico das top 20 cidades com mais estabelecimentos.
    
    Args:
        filtros_sql (str): Cláusula WHERE com filtros aplicados
    """
    st.markdown("### Top 20 Cidades com Mais Estabelecimentos")
    
    query_top_cidades = f"""
        SELECT * FROM vw_top_cidades_estabelecimentos
        {filtros_sql}
        ORDER BY total_estabelecimentos DESC
        LIMIT 20
    """
    
    try:
        df_top_cidades = fetch_data(query_top_cidades)
        
        if not df_top_cidades.empty:
            fig = px.bar(
                df_top_cidades.sort_values("total_estabelecimentos", ascending=True),
                x='total_estabelecimentos',
                y='cidade',
                orientation='h',
                labels={'cidade': 'Cidade', 'total_estabelecimentos': 'Total de Estabelecimentos'},
                color_discrete_sequence=['#667eea'],
                height=600
            )
            fig.update_layout(showlegend=False, margin=dict(l=50, r=50, t=30, b=50))
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Nenhum dado encontrado com os filtros selecionados.")
    except Exception as e:
        st.error(f"Erro ao gerar gráfico: {e}")

def show_details_table(filtros_sql: str):
    """
    Exibe a tabela de detalhes com opções de busca avançada e exportação.
    
    Args:
        filtros_sql (str): Cláusula WHERE com filtros aplicados
    """
    st.markdown("### Detalhes das Empresas e Estabelecimentos")
    
    query_tabela = f"""
        SELECT *
        FROM vw_estabelecimentos_empresas
        {filtros_sql.replace('e.', 'est.').replace('emp.', 'emp.')}
        LIMIT 1000
    """
    
    try:
        df_tabela = fetch_data(query_tabela)
        
        if not df_tabela.empty:
            # Formatação prévia dos dados
            if 'cnpj_completo' in df_tabela.columns:
                df_tabela['cnpj_formatado'] = df_tabela['cnpj_completo'].apply(format_cnpj)
            
            # Formatar datas se existirem
            date_columns = ['data_situacao_cadastral', 'data_inicio_atividade']
            for col in date_columns:
                if col in df_tabela.columns:
                    df_tabela[col] = pd.to_datetime(df_tabela[col], errors='coerce').dt.strftime('%d/%m/%Y')
            
            # Botão de download
            csv = df_tabela.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="📥 Baixar dados em CSV",
                data=csv,
                file_name="detalhes_estabelecimentos.csv",
                mime="text/csv"
            )
            
            st.write(f"Exibindo {len(df_tabela)} registros (máximo 1000)")
            
            # Área de busca avançada
            with st.expander("🔍 Busca Avançada", expanded=False):
                col1, col2 = st.columns([1, 3])
                with col1:
                    cnpj_busca = st.text_input(
                        "Buscar CNPJ específico:",
                        placeholder="Digite 14 dígitos",
                        help="Busca exata por CNPJ completo",
                        key='cnpj_busca_input'
                    )
                
                with col2:
                    nome_busca = st.text_input(
                        "Buscar por nome:",
                        placeholder="Nome Fantasia ou Razão Social",
                        help="Busca parcial por nome (mínimo 3 caracteres)",
                        key='nome_busca_input'
                    )
                
                if st.button("Executar Busca", key='busca_btn'):
                    st.session_state.executar_busca = True
                
                if st.button("Limpar Busca", key='limpar_busca_btn'):
                    st.session_state.executar_busca = False
                    df_tabela = fetch_data(query_tabela)  # Recarrega os dados originais

            # Lógica de busca avançada
            executar_busca = st.session_state.get('executar_busca', False)
            
            if executar_busca and (cnpj_busca or nome_busca):
                try:
                    where_clauses = []
                    
                    # Busca por CNPJ
                    if cnpj_busca:
                        cnpj_limpo = ''.join(filter(str.isdigit, cnpj_busca))
                        if len(cnpj_limpo) == 14:
                            where_clauses.append(f"cnpj_completo = '{cnpj_limpo}'")
                        else:
                            st.warning("CNPJ deve conter 14 dígitos")
                    
                    # Busca por nome
                    if nome_busca and len(nome_busca.strip()) >= 3:
                        nome = nome_busca.strip().replace("'", "''")
                        where_clauses.append(
                            f"(unaccent(nome_fantasia) ILIKE unaccent('%{nome}%') OR "
                            f"unaccent(razao_social) ILIKE unaccent('%{nome}%')"
                        )
                    
                    if where_clauses:
                        query_busca = f"""
                            SELECT * FROM vw_estabelecimentos_empresas
                            WHERE {' AND '.join(where_clauses)}
                            LIMIT 1000
                        """
                        df_busca = fetch_data(query_busca)
                        
                        if not df_busca.empty:
                            st.success(f"Encontrados {len(df_busca)} registros")
                            df_tabela = df_busca.copy()
                            if 'cnpj_completo' in df_tabela.columns:
                                df_tabela['cnpj_formatado'] = df_tabela['cnpj_completo'].apply(format_cnpj)
                        else:
                            st.warning("Nenhum resultado encontrado")
                    
                except Exception as e:
                    st.error(f"Erro na busca: {str(e)}")
            
            # Configuração da tabela AgGrid
            gb = GridOptionsBuilder.from_dataframe(df_tabela)
            
            # Configurações gerais
            gb.configure_pagination(
                paginationAutoPageSize=False,
                paginationPageSize=20
            )
            gb.configure_side_bar(filters_panel=True, columns_panel=True)
            gb.configure_default_column(
                filterable=True,
                sortable=True,
                resizable=True,
                editable=False,
                floatingFilter=True,
                suppressMenu=True
            )
            
            # Configurações específicas de colunas
            column_configs = {
                'cnpj_formatado': {
                    'header_name': "CNPJ",
                    'width': 200,
                    'pinned': 'left',
                    'filter': 'agTextColumnFilter',
                    'filter_params': {'debounceMs': 500}
                },
                'nome_fantasia': {
                    'header_name': "Nome Fantasia",
                    'flex': 2,
                    'filter': 'agTextColumnFilter',
                    'filter_params': {
                        'caseSensitive': False,
                        'debounceMs': 300
                    }
                },
                'razao_social': {
                    'header_name': "Razão Social",
                    'flex': 2,
                    'filter': 'agTextColumnFilter'
                },
                'data_situacao_cadastral': {
                    'header_name': "Situação Cadastral",
                    'width': 120
                }
            }
            
            for col, config in column_configs.items():
                if col in df_tabela.columns:
                    gb.configure_column(field=col, **config)
            
            grid_options = gb.build()
            
            # Exibe a tabela
            AgGrid(
                df_tabela,
                gridOptions=grid_options,
                update_mode=GridUpdateMode.FILTERING_CHANGED | GridUpdateMode.SORTING_CHANGED,
                fit_columns_on_grid_load=False,
                height=600,
                theme='streamlit',
                allow_unsafe_jscode=True,
                custom_css={
                    ".ag-header-cell-label": {"justify-content": "center"},
                    ".ag-cell-value": {"display": "flex", "align-items": "center"}
                }
            )
            
        else:
            st.warning("Nenhum dado encontrado com os filtros selecionados.")
    except Exception as e:
        st.error(f"Erro ao carregar dados da tabela: {str(e)}")

def show_footer():
    """Exibe o rodapé do dashboard com informações de fonte de dados."""
    st.markdown("---")
    st.markdown(f"""
    <div style="text-align: center; color: gray;">
        <strong>Fonte de Dados</strong><br>
        <br>
        <ul style="list-style:none; padding:0;">
            <li><a href="https://www.gov.br/receitafederal/" target="_blank">RFB - Receita Federal do Brasil</a></li>
        </ul>
        <br>
        <p>Última atualização: {get_current_time()}</p>
        <p>Tempo total de processamento: {(time.time() - tempo_inicio):.2f} segundos</p>
        <small>📅 Dados atualizados periodicamente
        
    </div>
    """, unsafe_allow_html=True)

# =============================================
# EXECUÇÃO PRINCIPAL
# =============================================

def main():
    """Função principal do aplicativo."""
    
    # Carrega ícones Bootstrap
    st.markdown(
        '<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap-icons@1.11.3/font/bootstrap-icons.min.css">',
        unsafe_allow_html=True
    )
    
    # Título e descrição
    st.title("Dashboard de Estabelecimentos CNPJ")
    st.markdown("""
    Este dashboard apresenta informações sobre estabelecimentos registrados no CNPJ, permitindo filtrar por UF, município, CNAE e outras características.  
    **Fonte:** [Receita Federal - Dados Abertos](https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/?C=N;O=D)
    """)
    
    # Carrega dados estáticos
    static_data = get_static_data()
    
    # Verifica se os dados foram carregados
    if not static_data["ufs"] or static_data["municipios"].empty:
        st.error("Falha ao carregar dados estáticos. Por favor, recarregue a página.")
        st.stop()
    
    # Configura filtros na sidebar
    filtros_sql = setup_sidebar_filters(static_data)
    
    # Exibe componentes principais
    show_metrics_cards(filtros_sql)
    show_top_cities_chart(filtros_sql)
    show_details_table(filtros_sql)
    show_footer()
    
    # Exibe tempo total de execução
    tempo_total = time.time() - tempo_inicio
    st.success(f"⏱️ Tempo total de carregamento: {tempo_total:.2f} segundos")

if __name__ == "__main__":
    main()