import streamlit as st
import pandas as pd
import psycopg2
import plotly.express as px
from datetime import datetime
import pytz
import time
from contextlib import contextmanager
from functools import wraps
import logging

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

# Início da contagem de tempo
tempo_inicio = time.time()

# =============================================
# CONFIGURAÇÕES INICIAIS
# =============================================
st.set_page_config(
    page_title="Dashboard CCEE - Agentes do Mercado Livre de Energia",
    page_icon="⚡",
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
    
    substituicoes = {
        'Ã\u008d': 'Í',
        'Ã\u0089': 'É',
        'Ã\u0087': 'Ç',
        'Ã\u0083': 'Ã',
        'Ã\u0081': 'Á',
        'Ã\u0095': 'Õ',
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
    """Estabelece conexão com o banco de dados com tratamento de erros"""
    try:
        logger.info("Estabelecendo conexão com o banco de dados")
        conn = psycopg2.connect(**Config.DB_CONFIG)
        conn.set_client_encoding('UTF8')
        return conn
    except Exception as e:
        st.error(f"Erro ao conectar ao banco de dados: {str(e)}")
        logger.error(f"Erro na conexão com o banco: {str(e)}")
        return None

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

def format_cnpj(cnpj: str) -> str:
    """Formata CNPJ com máscara."""
    cnpj = str(cnpj).zfill(14)
    return f"{cnpj[:2]}.{cnpj[2:5]}.{cnpj[5:8]}/{cnpj[8:12]}-{cnpj[12:14]}"

def get_current_time() -> str:
    """Retorna a data/hora atual formatada."""
    fuso = pytz.timezone(Config.TIMEZONE)
    return datetime.now(fuso).strftime("%d/%m/%Y %H:%M:%S")

# =============================================
# CARREGAMENTO DE DADOS ESTÁTICOS E DINÂMICOS
# =============================================

@st.cache_data(ttl=600, show_spinner="Carregando dados...")
def fetch_data(query: str, params=None) -> pd.DataFrame:
    """Executa consulta SQL e retorna DataFrame com cache"""
    try:
        with get_db_connection() as conn:
            logger.info(f"Executando consulta: {query[:100]}...")
            return pd.read_sql(query, conn, params=params)
    except Exception as e:
        st.error(f"Erro ao executar consulta: {e}")
        logger.error(f"Erro na consulta: {str(e)}")
        return pd.DataFrame()

@timing_decorator
def get_static_data() -> dict:
    """Carrega dados estáticos para filtros"""
    data = {
        "ufs": [],
        "municipios": pd.DataFrame(),
        "cnaes": pd.DataFrame(),
        "portes": [],
        "anos": [],
        "meses_referencia": []
    }
    
    try:
        # Consulta para UFs
        data["ufs"] = fetch_data(
            "SELECT DISTINCT estado_uf FROM ccee_parcela_carga_consumo_2025 ORDER BY estado_uf"
        )["estado_uf"].dropna().tolist()
        
        # Consulta para municípios
        data["municipios"] = fetch_data(
            "SELECT DISTINCT cidade FROM ccee_parcela_carga_consumo_2025 WHERE cidade IS NOT NULL ORDER BY cidade"
        )
        
        # Consulta para CNAEs
        data["cnaes"] = fetch_data(
            "SELECT DISTINCT ramo_atividade FROM ccee_parcela_carga_consumo_2025 WHERE ramo_atividade IS NOT NULL ORDER BY ramo_atividade"
        )
        
        # Consulta para meses de referência
        data["meses_referencia"] = fetch_data(
            "SELECT DISTINCT mes_referencia FROM ccee_parcela_carga_consumo_2025 ORDER BY mes_referencia DESC"
        )["mes_referencia"].tolist()
        
    except Exception as e:
        st.error(f"Erro ao carregar dados estáticos: {e}")
        logger.error(f"Erro ao carregar dados estáticos: {str(e)}")
    
    return data

# Carrega dados estáticos
static_data = get_static_data()

# =============================================
# INTERFACE DO USUÁRIO - FILTROS
# =============================================

# --- Sidebar: Filtros Gerais ---
st.sidebar.subheader("Filtros Gerais")
st.sidebar.markdown("Filtre os dados conforme suas necessidades:")

# Filtros básicos
uf_filtro = st.sidebar.selectbox("UF", options=["Todos"] + static_data.get("ufs", []))
cidade_filtro = st.sidebar.selectbox("Cidade", options=["Todos"] + static_data.get("municipios", pd.DataFrame()).get("cidade", []).tolist())
ramo_filtro = st.sidebar.selectbox("Ramo de Atividade", options=["Todos"] + static_data.get("cnaes", pd.DataFrame()).get("ramo_atividade", []).tolist())

# --- Sidebar: Filtros Avançados ---
st.sidebar.header("Filtros Avançados")
mes_referencia = st.sidebar.selectbox("Mês Referência", options=["Todos"] + static_data.get("meses_referencia", []))
submercado_filtro = st.sidebar.selectbox("Submercado", ["Todos", "SUDESTE", "NORDESTE", "SUL", "NORTE"])

# =============================================
# CONSULTAS PRINCIPAIS
# =============================================

# --- Construção dos filtros SQL ---
where_clauses = []
params = {}

# Filtros básicos
if uf_filtro != "Todos":
    where_clauses.append("estado_uf = %(uf)s")
    params['uf'] = uf_filtro
    
if cidade_filtro != "Todos":
    where_clauses.append("cidade = %(cidade)s")
    params['cidade'] = cidade_filtro
    
if ramo_filtro != "Todos":
    where_clauses.append("ramo_atividade = %(ramo)s")
    params['ramo'] = ramo_filtro
    
if mes_referencia != "Todos":
    where_clauses.append("mes_referencia = %(mes)s")
    params['mes'] = mes_referencia
    
if submercado_filtro != "Todos":
    where_clauses.append("submercado = %(submercado)s")
    params['submercado'] = submercado_filtro

# Construção da cláusula WHERE
filtros_sql = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""

# Consulta para total de varejistas
query_total_varejistas = """
SELECT COUNT(DISTINCT cod_perfil_agente) AS total_varejistas
FROM ccee_lista_perfil_2025
WHERE sigla_perfil_agente IN ('CL', 'CE')
"""

# Consulta para consumo total
query_consumo_total = f"""
SELECT 
    SUM(consumo_total) AS consumo_total,
    COUNT(DISTINCT cod_perf_agente) AS total_agentes
FROM ccee_varejista_consumidor_2025
{filtros_sql}
"""

# Consulta para tabela de agentes
query_agentes = f"""
SELECT 
    lp.cod_agente AS "Cód. Agente",
    MAX(lp.sigla_perfil_agente) AS "Sigla do Agente",
    lp.nome_empresarial AS "Nome Empresarial",
    vc.mes_referencia AS "Mês Referência",
    SUM(vc.qtd_parcela_carga) AS "Parcelas de Carga",
    SUM(vc.consumo_total) AS "Consumo Total (MWh)",
    MAX(pc.data_migracao) AS "Data Migração"
FROM 
    ccee_lista_perfil_2025 lp
JOIN 
    ccee_varejista_consumidor_2025 vc ON vc.nome_empresarial = lp.nome_empresarial
JOIN
    ccee_parcela_carga_consumo_2025 pc ON pc.nome_empresarial = lp.nome_empresarial
{filtros_sql.replace('estado_uf', 'vc.estado_uf_carga').replace('submercado', 'vc.submercado_carga')}
GROUP BY
    lp.cod_agente,
    lp.nome_empresarial,
    vc.mes_referencia
ORDER BY 
    "Data Migração" DESC
LIMIT 1000;
"""

# =============================================
# LAYOUT PRINCIPAL
# =============================================

# --- Título do Dashboard ---
st.title("⚡Dashboard CCEE - Agentes do Mercado Livre de Energia")

st.markdown("""
Este dashboard apresenta informações sobre estabelecimentos registrados no CNPJ, permitindo filtrar por UF, município, Seção CNAE e Subclasse CNAE.  
Os dados são baseados em uma view materializada e otimizados para desempenho.  
Fonte dos dados do Mercado Livre de Energia: [CCEE](https://www.ccee.org.br/portal/mercado-livre-de-energia)
""")

# --- Cards com gradiente e ícones ---
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
    total_varejistas = fetch_data(query_total_varejistas).iloc[0]['total_varejistas']
    consumo_data = fetch_data(query_consumo_total, params)
    consumo_total = consumo_data.iloc[0]['consumo_total'] if not consumo_data.empty else 0
    total_agentes = consumo_data.iloc[0]['total_agentes'] if not consumo_data.empty else 0
    agora = get_current_time()
    
    st.markdown(f"""
    <div class="card-container">
        <div class="card">
            <div class="card-icon"><i class="bi bi-clock"></i></div>
            <div class="card-label">Última Atualização</div>
            <div class="card-value" style="font-size:20px;">{agora}</div>
        </div>
        <div class="card">
            <div class="card-icon"><i class="bi bi-shop"></i></div>
            <div class="card-label">Total Varejistas</div>
            <div class="card-value">{format_milhar(total_varejistas)}</div>
        </div>
        <div class="card">
            <div class="card-icon"><i class="bi bi-lightning-charge"></i></div>
            <div class="card-label">Consumo Total (MWh)</div>
            <div class="card-value">{format_milhar(consumo_total)}</div>
        </div>
        <div class="card">
            <div class="card-icon"><i class="bi bi-people"></i></div>
            <div class="card-label">Agentes Ativos</div>
            <div class="card-value">{format_milhar(total_agentes)}</div>
        </div>
    </div>
    """, unsafe_allow_html=True)
except Exception as e:
    st.error(f"Erro ao carregar métricas: {e}")
    logger.error(f"Erro ao carregar métricas: {str(e)}")

# --- Gráfico de evolução anual por perfil ---
query_perfis_ano = """
SELECT 
    EXTRACT(YEAR FROM data_importacao) AS ano,
    CASE 
        WHEN sigla_perfil_agente IN ('CL', 'CONSUMIDOR LIVRE') THEN 'Consumidor Livre'
        WHEN sigla_perfil_agente IN ('CE', 'CONSUMIDOR ESPECIAL') THEN 'Consumidor Especial'
        ELSE 'Outros'
    END AS perfil_simplificado,
    COUNT(DISTINCT cod_perfil_agente) AS total_agentes
FROM 
    ccee_lista_perfil_2025
GROUP BY 1, 2
ORDER BY 1, 2;
"""
df_perfis = fetch_data(query_perfis_ano)

if not df_perfis.empty:
    fig_perfis = px.line(
        df_perfis,
        x="ano",
        y="total_agentes",
        color="perfil_simplificado",
        markers=True,
        title="📈 Evolução Anual de Agentes por Perfil",
        labels={"ano": "Ano", "total_agentes": "Quantidade de Agentes", "perfil_simplificado": "Perfil"}
    )
    st.plotly_chart(fig_perfis, use_container_width=True)

# --- Tabela com dados dos agentes ---
st.subheader("📊 Tabela de Agentes")

dados_agentes = fetch_data(query_agentes, params)

if not dados_agentes.empty:
    # Botão de download
    csv = dados_agentes.to_csv(index=False).encode('utf-8')
    st.download_button(
        label="📥 Baixar dados em CSV",
        data=csv,
        file_name="dados_agentes_ccee.csv",
        mime="text/csv"
    )
    
    # Exibir tabela com st.data_editor para permitir filtros
    st.data_editor(
        dados_agentes,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Consumo Total (MWh)": st.column_config.NumberColumn(format="%.2f"),
            "Data Migração": st.column_config.DateColumn()
        }
    )
else:
    st.warning("Nenhum dado de agente encontrado para exibição.")

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

# Tempo total de execução
tempo_total = time.time() - tempo_inicio
st.success(f"⏱️ Tempo total de carregamento: {tempo_total:.2f} segundos")

# Mostrar logs de performance se necessário
if st.checkbox("Mostrar logs de performance"):
    if "performance_logs" in st.session_state:
        st.write("### Logs de Performance")
        for log in st.session_state.performance_logs:
            st.code(log)