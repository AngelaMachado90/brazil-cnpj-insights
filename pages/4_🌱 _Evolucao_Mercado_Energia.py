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

# Início da contagem de tempo
tempo_inicio = time.time()


# =============================================
# CONFIGURAÇÕES INICIAIS
# =============================================
st.set_page_config(
    page_title="Análise de Agentes CCEE - Consumidores Especiais",
    page_icon="🌱",  
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
    if pd.isna(cnpj):
        return ""
    cnpj = str(cnpj).zfill(14)
    return f"{cnpj[:2]}.{cnpj[2:5]}.{cnpj[5:8]}/{cnpj[8:12]}-{cnpj[12:14]}"

def format_telefone(tel: str) -> str:
    """Formata número de telefone."""
    if not tel or pd.isna(tel):
        return ""
    tel = ''.join(filter(str.isdigit, str(tel)))
    if len(tel) == 10:
        return f"({tel[:2]}) {tel[2:6]}-{tel[6:]}"
    elif len(tel) == 11:
        return f"({tel[:2]}) {tel[2:7]}-{tel[7:]}"
    return tel

def get_current_time() -> str:
    """Retorna a data/hora atual formatada."""
    fuso = pytz.timezone(Config.TIMEZONE)
    return datetime.now(fuso).strftime("%d/%m/%Y %H:%M:%S")

def get_tag_perfil(consumo: float) -> str:
    """Classifica o consumidor com base no consumo."""
    if pd.isna(consumo):
        return "🔵 Sem informação"
    if consumo > 10000: return "🔴 Grande Consumidor"
    elif consumo > 5000: return "🟡 Médio Consumidor"
    else: return "🟢 Pequeno Consumidor"

# =============================================
# CARREGAMENTO DE DADOS
# =============================================

@st.cache_data(ttl=600, show_spinner="Carregando dados...")
def fetch_data(query: str, params=None) -> pd.DataFrame:
    """Executa consulta SQL e retorna DataFrame com cache"""
    try:
        conn = get_db_connection()
        if conn is None:
            return pd.DataFrame()
        
        logger.info(f"Executando consulta: {query[:100]}...")
        df = pd.read_sql(query, conn)
        
        for col in df.select_dtypes(include=['object']).columns:
            df[col] = df[col].apply(corrigir_encoding)
            
        return df
    except Exception as e:
        st.error(f"Erro ao executar consulta: {e}")
        logger.error(f"Erro na consulta: {str(e)}")
        return pd.DataFrame()
 
@timing_decorator
def load_agent_data():
    """Carrega dados dos agentes com tratamento especial para consumidores"""
    query = """
        SELECT * FROM view_consumidores_especiais
    """
    
    df = fetch_data(query)
    
    if not df.empty:
        # Tratamento mais robusto para valores nulos
        df['consumo_total'] = df['consumo_total'].fillna(0).astype(float)
        df['estado_uf'] = df['estado_uf'].fillna('Não informado')
        df['municipio'] = df['municipio'].fillna('Não informado')
        df['descricao_cnae'] = df['descricao_cnae'].fillna('Não informado')
        df['submercado'] = df['submercado'].fillna('Não informado')
        
        # Formatação dos dados
        df["cnpj"] = df["cnpj"].apply(lambda x: format_cnpj(x) if pd.notna(x) and str(x).strip() != '' else 'Não informado')
        
        df["telefone"] = df.apply(
            lambda x: f"{format_telefone(x['telefone1'])} / {format_telefone(x['telefone2'])}".strip(" /") 
            if pd.notna(x['telefone1']) or pd.notna(x['telefone2']) 
            else 'Não informado',
            axis=1
        )
        
        df['email'] = df['email'].fillna('Não informado')
        df['tag_perfil'] = df['consumo_total'].apply(get_tag_perfil)
    
    return df
    
# =============================================
# INTERFACE PRINCIPAL
# =============================================

def main():
    tempo_inicio = time.time()
    dados_agentes = load_agent_data()
        
    if dados_agentes.empty:
        st.error("""
        Não foi possível carregar os dados. Verifique:
        1. Conexão com o banco de dados
        2. Existência de dados nas tabelas:
           - ccee_lista_perfil_2025
           - ccee_parcela_carga_consumo_2025
           - rfb_estabelecimentos
           - cnae_10
        3. Permissões de acesso
        """)
        
        if st.button("🔄 Tentar novamente"):
            st.rerun()
        
        st.stop()
    
    # --- Sidebar: Filtros Inteligentes ---
    with st.sidebar:
        st.header("🔍 Filtros")
        
        # Filtro de UF - apenas mostrar UFs com dados
        ufs_disponiveis = ["Todos"] + sorted(
            [uf for uf in dados_agentes["estado_uf"].unique() 
            if uf != 'Não informado' and pd.notna(uf)]
        )
        uf_selecionada = st.selectbox(
            "UF",
            ufs_disponiveis,
            index=0
        )
        
        # Filtro de município - dinâmico baseado na UF selecionada
        municipios_disponiveis = ["Todos"]
        if uf_selecionada != "Todos":
            municipios_disponiveis += sorted(
                [m for m in dados_agentes[dados_agentes["estado_uf"] == uf_selecionada]["municipio"].unique()
                if m != 'Não informado' and pd.notna(m)]
            )
        else:
            municipios_disponiveis += sorted(
                [m for m in dados_agentes["municipio"].unique()
                if m != 'Não informado' and pd.notna(m)]
            )
        
        municipio_selecionado = st.selectbox(
            "Município",
            municipios_disponiveis,
            index=0
        )

    # --- Visualização dos dados ---
    st.title(f"🔌 Painel de Consumidores Especiais CCEE")
    st.caption(f"Última atualização: {get_current_time()}")

    # Métricas interativas
    with st.expander("📊 Métricas Principais", expanded=True):
        cols = st.columns(4)
        consumo_total = df_filtrado['consumo_total'].sum()
        media_consumo = consumo_total / len(df_filtrado) if len(df_filtrado) > 0 else 0
        
        with cols[0]:
            st.metric("Total de Agentes", len(df_filtrado), 
                     help="Número total de consumidores especiais")
        with cols[1]:
            st.metric("Consumo Total", f"{consumo_total:,.2f} MWh", 
                     delta=f"{media_consumo:,.2f} MWh médios",
                     help="Consumo energético agregado")
        with cols[2]:
            st.metric("Estados Ativos", df_filtrado['estado_uf'].nunique(),
                     help="Distribuição geográfica")
        with cols[3]:
            st.metric("Atividades", df_filtrado['descricao_cnae'].nunique(),
                     help="Diversidade de atividades econômicas")

    # Gráficos de análise
    tab1, tab2, tab3 = st.tabs(["📈 Consumo vs. Tempo", "🏭 Por Atividade", "🗺️ Por Localização"])
    
    # Na função main(), modifique a exibição dos gráficos:

    with tab1:
        st.subheader("Análise por Atividade Econômica")
        if not df_filtrado.empty and 'descricao_cnae' in df_filtrado.columns:
            # Filtrar apenas atividades informadas
            df_cnae = df_filtrado[df_filtrado['descricao_cnae'] != 'Não informado']
            
            if not df_cnae.empty:
                consumo_por_cnae = df_cnae.groupby('descricao_cnae')['consumo_total'].sum().reset_index()
                consumo_por_cnae = consumo_por_cnae.sort_values('consumo_total', ascending=False).head(20)
                
                fig = px.bar(
                    consumo_por_cnae,
                    x='descricao_cnae',
                    y='consumo_total',
                    title='Top 20 Atividades Econômicas por Consumo',
                    labels={'descricao_cnae': 'Atividade', 'consumo_total': 'Consumo Total (MWh)'},
                    color='consumo_total',
                    color_continuous_scale='Viridis'
                )
                fig.update_layout(
                    xaxis_tickangle=-45,
                    height=500,
                    plot_bgcolor='rgba(0,0,0,0)',
                    paper_bgcolor='rgba(0,0,0,0)'
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.warning("Nenhuma atividade econômica informada nos dados filtrados")
        else:
            st.warning("Dados insuficientes para exibir análise por atividade")
        
    with tab2:
        st.subheader("Análise por Atividade Econômica")
        if not df_filtrado.empty and 'descricao_cnae' in df_filtrado.columns:
            consumo_por_cnae = df_filtrado.groupby('descricao_cnae')['consumo_total'].sum().reset_index()
            consumo_por_cnae = consumo_por_cnae.sort_values('consumo_total', ascending=False).head(20)
            
            fig = px.bar(
                consumo_por_cnae,
                x='descricao_cnae',
                y='consumo_total',
                title='Top 20 Atividades Econômicas por Consumo',
                labels={'descricao_cnae': 'Atividade', 'consumo_total': 'Consumo Total (MWh)'},
                color='consumo_total',
                color_continuous_scale='Viridis'
             
            )
            fig.update_layout(
                xaxis_tickangle=-45,
                height=500,
                plot_bgcolor='rgba(0,0,0,0)',
                paper_bgcolor='rgba(0,0,0,0)'
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning("Dados insuficientes para exibir análise por atividade")
    
    with tab3:
        st.subheader("Análise Geográfica")
        if not df_filtrado.empty and 'estado_uf' in df_filtrado.columns:
            contagem_por_uf = df_filtrado['estado_uf'].value_counts().reset_index()
            contagem_por_uf.columns = ['UF', 'Quantidade']
            
            fig_uf = px.choropleth(
                contagem_por_uf,
                locations='UF',
                locationmode='BR-UF',
                color='Quantidade',
                scope='south america',
                title='Distribuição de Agentes por UF',
                color_continuous_scale='Bluered',
                height=500
            )
            st.plotly_chart(fig_uf, use_container_width=True)
        else:
            st.warning("Dados insuficientes para exibir análise geográfica")

    # Tabela principal
    st.subheader("📋 Dados Detalhados dos Agentes")
    
    if not df_filtrado.empty:
        st.dataframe(
            df_filtrado[[
                'tag_perfil', 'nome_empresarial', 'cnpj', 'sigla_perfil_agente', 
                'estado_uf', 'municipio', 'descricao_cnae', 'consumo_total', 
                'telefone', 'email'
            ]].rename(columns={
                'tag_perfil': 'Perfil',
                'nome_empresarial': 'Nome Empresarial',
                'cnpj': 'CNPJ',
                'sigla_perfil_agente': 'Sigla',
                'estado_uf': 'UF',
                'municipio': 'Município',
                'descricao_cnae': 'Atividade',
                'consumo_total': 'Consumo (MWh)',
                'telefone': 'Telefone',
                'email': 'E-mail'
            }),
            use_container_width=True,
            height=600,
            hide_index=True,
            column_config={
                "Perfil": st.column_config.TextColumn(width="small"),
                "Consumo (MWh)": st.column_config.NumberColumn(format="%.2f"),
                "CNPJ": st.column_config.TextColumn(width="medium")
            }
        )

        # Botão para download
        csv = df_filtrado.to_csv(index=False, encoding='utf-8-sig')
        st.download_button(
            label="📥 Baixar dados como CSV",
            data=csv,
            file_name=f"consumidores_especiais_{datetime.now().strftime('%Y%m%d')}.csv",
            mime="text/csv",
            help="Exportar todos os dados filtrados para análise externa"
        )
    else:
        st.warning("Nenhum dado encontrado com os filtros selecionados")

    # --- Rodapé ---
    st.markdown("---")
    st.markdown("""
    <div style="text-align: center; color: gray;">
        <strong>Fonte de Dados</strong><br>
        <ul style="list-style:none; padding:0;">
            <li><a href="https://www.ccee.org.br/portal/mercado-livre-de-energia" target="_blank">CCEE - Câmara de Comercialização de Energia Elétrica</a></li>
        </ul>
        <small>📅 Dados atualizados periodicamente</smal> 
        
        
    </div>
    """, unsafe_allow_html=True)

    # Tempo total de execução
    tempo_total = time.time() - tempo_inicio
    st.success(f"⏱️ Tempo total de carregamento: {tempo_total:.2f} segundos")

if __name__ == "__main__":
    main()