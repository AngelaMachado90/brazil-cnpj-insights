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
# Configuração do tema Streamlit
st.markdown("""
<style>
    .stApp {
        background-color: #f0f2f5;
    }           
    .stButton>button {
        background-color: #667eea;
        color: white;
        border-radius: 5px;
        padding: 10px 20px;
        border: none;
        font-weight: bold;
    }
    .stButton>button:hover {
        background-color: #5a6bcf;
    }
    .stTextInput>div>input {
        border-radius: 5px;
        border: 1px solid #ccc;
        padding: 10px;
    }
    .stTextInput>div>input:focus {
        border-color: #667eea;
        box-shadow: 0 0 5px rgba(102, 126,
    234, 0.5);  
    }
</style>
""", unsafe_allow_html=True)
# Ícones Bootstrap
st.markdown(
    '<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap-icons@1.11.3/font/bootstrap-icons.min.css">',
    unsafe_allow_html=True
)

# ====================================================================================================================
# FUNÇÕES UTILITÁRIAS
# ====================================================================================================================

@st.cache_resource(ttl=3600)  # Cache por 1 hora
def get_db_connection():
    """Estabelece e retorna uma nova conexão com o banco de dados"""
    try:
        conn = psycopg2.connect(**Config.DB_CONFIG)
        conn.autocommit = True
        return conn
    except Exception as e:
        st.error(f"Erro ao conectar ao banco de dados: {str(e)}")
        raise

@contextmanager
def db_connection():
    """Gerenciador de contexto para conexões com o banco"""
    conn = None
    try:
        conn = get_db_connection()
        yield conn
    except psycopg2.InterfaceError as e:
        if "connection already closed" in str(e):
            st.warning("Conexão com o banco de dados foi fechada. Tentando reconectar...")
            try:
                conn = get_db_connection()
                yield conn
            except Exception as e:
                st.error(f"Erro ao reconectar: {str(e)}")
                raise
        else:
            st.error(f"Erro de conexão: {str(e)}")
            raise
    except Exception as e:
        st.error(f"Erro inesperado: {str(e)}")
        raise
    finally:
        if conn and not conn.closed:
            conn.close()

def check_connection(conn):
    """Verifica se a conexão está ativa"""
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            return True
    except:
        return False

def format_cnpj(cnpj: str) -> str:
    """Formata CNPJ com máscara."""
    cnpj = str(cnpj).zfill(14)
    return f"{cnpj[:2]}.{cnpj[2:5]}.{cnpj[5:8]}/{cnpj[8:12]}-{cnpj[12:14]}"

def format_milhar(n: int) -> str:
    """Formata número com separador de milhar."""
    return f"{n:,.0f}".replace(",", ".")

def get_current_time() -> str:
    """Retorna a data/hora atual formatada."""
    fuso = pytz.timezone(Config.TIMEZONE)
    return datetime.now(fuso).strftime("%d/%m/%Y %H:%M:%S")

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

# =============================================
# CONSULTAS SQL
# =============================================

# Consultas para os cards de resumo
query_data_importacao = f"""
SELECT 
    MAX(data_importacao) AS data_importacao
FROM 
    ccee_parcela_carga_consumo_2025
WHERE 
    data_importacao IS NOT NULL;
"""

def query_migracao_mercado_livre(conn, anos=5):
    """Consulta empresas que migraram para o mercado livre"""
    try:
        if conn.closed or not check_connection(conn):
            st.warning("Conexão perdida. Reconectando...")
            conn = get_db_connection()

        query = f"""
        WITH dados_migracao AS (
            SELECT 
                TO_CHAR(data_migracao, 'YYYY-MM') AS ano_mes,
                COUNT(DISTINCT cnpj_carga) AS quantidade_migracoes,
                0 AS eh_total
            FROM 
                public.ccee_parcela_carga_consumo_2025
            WHERE 
                data_migracao IS NOT NULL
                AND data_migracao >= (CURRENT_DATE - INTERVAL '{anos} years')
            GROUP BY 
                TO_CHAR(data_migracao, 'YYYY-MM')
            
            UNION ALL
            
            SELECT 
                'TOTAL' AS ano_mes,
                COUNT(DISTINCT cnpj_carga) AS quantidade_migracoes,
                1 AS eh_total
            FROM 
                public.ccee_parcela_carga_consumo_2025
            WHERE 
                data_migracao IS NOT NULL
                AND data_migracao >= (CURRENT_DATE - INTERVAL '{anos} years')
        )
        
        SELECT ano_mes, quantidade_migracoes
        FROM dados_migracao
        ORDER BY 
            eh_total,
            ano_mes;
        """
        return pd.read_sql(query, conn)
    except Exception as e:
        st.error(f"Erro na consulta: {str(e)}")
        raise

def query_detalhe_empresas(conn, anos=5, uf=None, porte=None):
    """Consulta detalhada das empresas com informações de sócios"""
    try:
        if conn.closed or not check_connection(conn):
            st.warning("Conexão perdida. Reconectando...")
            conn = get_db_connection()

        query = f"""
        SELECT 
            c.cnpj_carga,
            es.cnpj_completo,
            c.data_migracao,
            TO_CHAR(c.data_migracao, 'YYYY-MM') AS ano_mes,
            es.nome_fantasia,
            es.uf,
            es.municipio,
            es.cnae_fiscal_principal,
            es.email as "EMAIL",
            CASE 
                WHEN es.ddd1 IS NOT NULL AND es.telefone1 IS NOT NULL THEN CONCAT('(', es.ddd1, ') ', es.telefone1)
                WHEN es.ddd1 IS NULL AND es.telefone1 IS NOT NULL THEN es.telefone1
                WHEN es.ddd1 IS NOT NULL AND es.telefone1 IS NULL THEN CONCAT('(', es.ddd1, ')')
                ELSE NULL
            END AS TELEFONE01,
            CASE 
                WHEN es.ddd2 IS NOT NULL AND es.telefone2 IS NOT NULL THEN CONCAT('(', es.ddd2, ') ', es.telefone2)
                WHEN es.ddd2 IS NULL AND es.telefone2 IS NOT NULL THEN es.telefone2
                WHEN es.ddd2 IS NOT NULL AND es.telefone2 IS NULL THEN CONCAT('(', es.ddd2, ')')
                ELSE NULL
            END AS TELEFONE02,
            CASE 
                WHEN es.ddd_fax IS NOT NULL AND es.fax IS NOT NULL THEN CONCAT('(', es.ddd_fax, ') ', es.fax)
                WHEN es.ddd_fax IS NULL AND es.fax IS NOT NULL THEN es.fax
                WHEN es.ddd_fax IS NOT NULL AND es.fax IS NULL THEN CONCAT('(', es.ddd_fax, ')')
                ELSE NULL
            END AS TELEFONE03,
            s.nome_socio AS nome_socio,
            s.cnpj_cpf_socio AS cnpj_socio,
            s.qualificacao_socio AS qualificacao_socio,
            s.data_entrada_sociedade AS data_entrada_socio,
            s.representante_legal AS representante_legal,
            s.nome_representante AS nome_representante,
            s.faixa_etaria AS faixa_etaria
        FROM 
            ccee_parcela_carga_consumo_2025 c
        JOIN 
            rfb_estabelecimentos es 
            ON LPAD(SPLIT_PART(c.cnpj_carga::text, '.', 1), 14, '0') = es.cnpj_completo
        LEFT JOIN 
            rfb_socios s 
            ON es.cnpj_basico = s.cnpj_basico
        WHERE 
            c.data_migracao IS NOT NULL
            AND c.data_migracao >= (CURRENT_DATE - INTERVAL '{anos} years')
        """
        
        conditions = []
        if uf and uf != "Todos":
            conditions.append(f"es.uf = '{uf}'")
        if porte and porte != "Todos":
            conditions.append(f"es.porte = '{porte}'")
        
        if conditions:
            query += " AND " + " AND ".join(conditions)
        
        query += " ORDER BY c.data_migracao DESC, es.nome_fantasia, s.nome_socio;"
        
        return pd.read_sql(query, conn)
    except Exception as e:
        st.error(f"Erro na consulta: {str(e)}")
        raise

# =============================================
# INTERFACE DO USUÁRIO
# =============================================

st.title("⚡ Empresas que Migraram para o Mercado Livre de Energia")
st.markdown("""
Dashboard que monitora empresas que migraram para o Mercado Livre de energia nos últimos anos.
**Fonte:** [Receita Federal - Dados Abertos](https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/?C=N;O=D)
**Fonte:** [CCEE - Dados Abertos](https://dadosabertos.ccee.org.br/dataset/parcela_carga_consumo/resource/c88d04a6-fe42-413b-b7bf-86e390494fb0)
""")

with st.sidebar:
    st.header("Filtros")
    anos_analise = st.slider("Período de análise (anos)", 1, 10, 5)
    porte_filtro = st.selectbox("Porte da empresa", ["Todos", "Micro Empresa", "Pequeno Porte", "Médio Porte", "Grande Porte"])


# --- Cards de métricas ---
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

# Buscar dados para os cards
try:
    agora = get_current_time()
    data_importacao = fetch_data(query_data_importacao).iloc[0]['data_importacao']
    total_empresas_migradas = fetch_data("SELECT COUNT(DISTINCT cnpj_carga) AS total FROM ccee_parcela_carga_consumo_2025 WHERE data_migracao IS NOT NULL").iloc[0]['total']
    total_ultimos_anos = fetch_data(f"""
    SELECT COUNT(DISTINCT cnpj_carga) AS total
    FROM ccee_parcela_carga_consumo_2025
    WHERE data_migracao IS NOT NULL
    AND data_migracao >= (CURRENT_DATE - INTERVAL '{anos_analise} years')
    """).iloc[0]['total']
    
    st.markdown(f"""
    <div class="card-container">
        <div class="card">
            <div class="card-icon"><i class="bi bi-clock"></i></div>
            <div class="card-label">Última Atualização</div>
            <div class="card-value" style="font-size:20px;">{agora}</div>
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


with st.spinner("Buscando dados de migração para o Mercado Livre..."):
    try:
        with db_connection() as conn:
            df_migracao = query_migracao_mercado_livre(conn, anos_analise)
            # ===== Exibição dos dados de migração ===== #
            if not df_migracao.empty:
                df_grafico = df_migracao[df_migracao['ano_mes'] != 'TOTAL']
                df_total = df_migracao[df_migracao['ano_mes'] == 'TOTAL']
                total_empresas = df_total['quantidade_migracoes'].values[0] if not df_total.empty else 0
                
                df_grafico['data_formatada'] = pd.to_datetime(df_grafico['ano_mes'], errors='coerce')
                df_grafico = df_grafico.sort_values('data_formatada')
                
                col1, col2 = st.columns(2)
                col1.metric("Total de Empresas Migradas", format_milhar(total_empresas))
                
                #st.subheader("Evolução das Migrações para o Mercado Livre")
                #st.markdown(f"Análise dos últimos {anos_analise} anos")
                #st.markdown("Este gráfico mostra a evolução mensal do número de empresas que migraram para o Mercado Livre de Energia.")
                #st.markdown("As empresas são consideradas migradas quando possuem uma data de migração registrada no sistema da CCEE.")
                #st.markdown("O total de empresas migradas é calculado com base no número de CNPJs distintos que realizaram a migração.")
                #st.markdown("O último mês exibido no gráfico representa o mês mais recente com dados disponíveis.")
                
                #=== Exibição dos dados de migração - GRAFICO COLUNAS, EVOLUTIVO MES E ANO ===== #
                # ===== Exibição dos dados de migração ===== #
            if not df_migracao.empty:
                df_grafico = df_migracao[df_migracao['ano_mes'] != 'TOTAL']
                df_total = df_migracao[df_migracao['ano_mes'] == 'TOTAL']
                total_empresas = df_total['quantidade_migracoes'].values[0] if not df_total.empty else 0
                
                # Converter para datetime e ordenar
                df_grafico['data_formatada'] = pd.to_datetime(df_grafico['ano_mes'], format='%Y-%m')
                df_grafico = df_grafico.sort_values('data_formatada')
                
                df_grafico['ano'] = df_grafico['data_formatada'].dt.year
                df_agrupado = df_grafico.groupby('ano')['quantidade_migracoes'].sum().reset_index()
                
                # Criar colunas para métricas
                col1, col2 = st.columns(2)
                col1.metric("Total de Empresas Migradas", format_milhar(total_empresas))
                
                if not df_grafico.empty:
                    ultimo_mes = df_grafico.iloc[-1]
                    col2.metric(f"Migrações em {ultimo_mes['ano_mes']}", 
                            format_milhar(ultimo_mes['quantidade_migracoes']))
                
                # Gráfico de barras VERTICAIS
                st.subheader("Evolução das Migrações para o Mercado Livre")
                
                fig = px.bar(
                    df_grafico,
                    x='ano_mes',  # Período no eixo X
                    y='quantidade_migracoes',  # Quantidade no eixo Y
                    labels={'ano_mes': 'Período (Mês-Ano)', 'quantidade_migracoes': 'Empresas Migradas'},
                    title='Evolução Mensal das Migrações para o Mercado Livre',
                    color='quantidade_migracoes',
                    color_continuous_scale='Blues'
                )
                
                # Ajustes de layout para gráfico vertical
                fig.update_layout(
                    xaxis={'categoryorder': 'array', 'categoryarray': df_grafico['ano_mes'].tolist()},
                    height=500,
                    xaxis_title="Período",
                    yaxis_title="Quantidade de Empresas Migradas",
                    showlegend=False,
                    margin={'t': 50}
                )
                
                # Adicionar valores nas barras
                fig.update_traces(
                    texttemplate='%{y:,}',
                    textposition='outside'
                )
                
                # Rotacionar labels do eixo X para melhor legibilidade
                fig.update_xaxes(tickangle=45)

                # Exibir gráfico
                #Teste ano
                st.markdown("Este gráfico mostra a evolução mensal do número de empresas que migraram para o Mercado Livre de Energia nos últimos anos.")

                fig = px.bar(
                    df_agrupado,
                    x='ano',
                    y='quantidade_migracoes',
                    title='Migrações por Ano'
                )
                
                st.plotly_chart(fig, use_container_width=True)
                # Tabela de detalhes
                st.subheader("Detalhes das Migrações por Período")
                df_tabela = df_grafico[['ano_mes', 'quantidade_migracoes']].copy()
                df_tabela = df_tabela.rename(columns={'ano_mes': 'Período', 'quantidade_migracoes': 'Empresas Migradas'})
                total_row = pd.DataFrame({'Período': ['TOTAL'], 'Empresas Migradas': [total_empresas]})
                df_tabela = pd.concat([df_tabela, total_row], ignore_index=True)
                
                st.dataframe(
                    df_tabela,
                    use_container_width=True,
                    hide_index=True
                )
                
                csv = df_migracao.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="📥 Baixar dados completos",
                    data=csv,
                    file_name=f"migracao_mercado_livre_ultimos_{anos_analise}_anos.csv",
                    mime="text/csv"
                )
                
                st.subheader("📋 Detalhamento das Empresas Migradas")

                with st.expander("🔍 Filtros Avançados", expanded=True):
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        uf_filtro = st.selectbox("Filtrar por UF", ["Todos"] + sorted(pd.read_sql("SELECT DISTINCT uf FROM rfb_estabelecimentos ORDER BY uf", conn)['uf'].tolist()))
                    with col2:
                        porte_filtro_detalhes = st.selectbox("Filtrar por Porte", ["Todos", "MEI", "ME", "EPP", "Grande"], key="porte_detalhes")
                    with col3:
                        data_inicio, data_fim = st.date_input(
                            "Período de migração",
                            value=[datetime.now() - timedelta(days=365*anos_analise), datetime.now()],
                            min_value=datetime.now() - timedelta(days=365*10),
                            max_value=datetime.now()
                        )

                with st.spinner("Buscando detalhes das empresas..."):
                    df_detalhes = query_detalhe_empresas(
                        conn, 
                        anos=anos_analise,
                        uf=uf_filtro if uf_filtro != "Todos" else None,
                        porte=porte_filtro_detalhes if porte_filtro_detalhes != "Todos" else None
                    )
                    
                    if not df_detalhes.empty:
                        df_detalhes['data_migracao'] = pd.to_datetime(df_detalhes['data_migracao']).dt.date
                        data_inicio = pd.to_datetime(data_inicio).date()
                        data_fim = pd.to_datetime(data_fim).date()
                        
                        df_detalhes = df_detalhes[
                            (df_detalhes['data_migracao'] >= data_inicio) & 
                            (df_detalhes['data_migracao'] <= data_fim)
                        ]
                        
                        df_detalhes['cnpj_formatado'] = df_detalhes['cnpj_completo'].apply(format_cnpj)
                        
                        possible_columns = {
                            'cnpj_formatado': 'CNPJ',
                            'nome_fantasia': 'Nome Fantasia',
                            'uf': 'UF',
                            'municipio': 'Município',
                            'data_migracao': 'Data Migração',
                            'ano_mes': 'Ano/Mês',
                            'cnae_fiscal_principal': 'CNAE Principal',
                            'EMAIL': 'E-mail',
                            'TELEFONE01': 'Telefone 1',
                            'TELEFONE02': 'Telefone 2',
                            'TELEFONE03': 'Fax',
                            'nome_socio': 'Nome do Sócio',
                            'cnpj_socio': 'CNPJ/CPF do Sócio',
                            'qualificacao_socio': 'Qualificação do Sócio',
                            'data_entrada_socio': 'Data de Entrada',
                            'representante_legal': 'Representante Legal',
                            'nome_representante': 'Nome do Representante',
                            'faixa_etaria': 'Faixa Etária'
                        }
                        
                        cols_to_show = {col: name for col, name in possible_columns.items() if col in df_detalhes.columns}
                        df_display = df_detalhes[list(cols_to_show.keys())].rename(columns=cols_to_show)
                        
                        st.success(f"{len(df_detalhes.drop_duplicates('cnpj_completo'))} empresas encontradas com {len(df_detalhes)} registros de sócios.")
                        
                        column_config = {
                            "CNPJ": st.column_config.TextColumn(width="medium"),
                            "Nome Fantasia": st.column_config.TextColumn(width="large"),
                            "Data Migração": st.column_config.DateColumn(format="DD/MM/YYYY"),
                            "E-mail": st.column_config.TextColumn(width="large"),
                            "Nome do Sócio": st.column_config.TextColumn(width="large"),
                            "CNPJ/CPF do Sócio": st.column_config.TextColumn(width="medium"),
                            "Qualificação do Sócio": st.column_config.TextColumn(width="medium"),
                            "Data de Entrada": st.column_config.DateColumn(format="DD/MM/YYYY")
                        }
                        
                        if 'Telefone 1' in df_display.columns:
                            column_config["Telefone 1"] = st.column_config.TextColumn(width="small")
                        if 'Telefone 2' in df_display.columns:
                            column_config["Telefone 2"] = st.column_config.TextColumn(width="small")
                        if 'Fax' in df_display.columns:
                            column_config["Fax"] = st.column_config.TextColumn(width="small")
                        
                        st.dataframe(
                            df_display,
                            use_container_width=True,
                            height=600,
                            column_config=column_config,
                            hide_index=True
                        )
                        
                        csv_detalhes = df_detalhes.to_csv(index=False).encode('utf-8')
                        st.download_button(
                            label="📥 Baixar detalhes completos",
                            data=csv_detalhes,
                            file_name=f"detalhes_empresas_migracao_ml_{anos_analise}_anos.csv",
                            mime="text/csv",
                            key="download_detalhes"
                        )
                    else:
                        st.warning("Nenhuma empresa encontrada com os critérios selecionados.")

            else:
                st.warning("Nenhuma empresa encontrada com os critérios selecionados.")
                
    except psycopg2.OperationalError as e:
        st.error("Erro de conexão com o banco de dados. Por favor, tente novamente mais tarde.")
        st.stop()        
    except Exception as e:
        st.error(f"Erro ao consultar dados: {str(e)}")
        st.stop()

st.markdown("---")
st.markdown(f"""
<div style="text-align: center; color: gray;">
    <p>Última atualização: {get_current_time()}</p>
    <p>Dados: CCEE (Câmara de Comercialização de Energia Elétrica) e RFB</p>
    <href="
</div>
""", unsafe_allow_html=True)