import streamlit as st
import pandas as pd
import psycopg2
import plotly.express as px
from datetime import datetime, timedelta
import pytz
import logging
from functools import wraps
import time

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
    page_title="Análise Avançada por Perfil",
    page_icon="👷‍♂️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Fuso horário
fuso = pytz.timezone(Config.TIMEZONE)
agora = datetime.now(fuso).strftime("%d/%m/%Y %H:%M:%S")

# =============================================
# FUNÇÕES UTILITÁRIAS
# =============================================

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

@st.cache_data(ttl=3600, show_spinner="Carregando dados...")
def fetch_data(query: str, params=None) -> pd.DataFrame:
    """Executa consulta SQL e retorna DataFrame com cache"""
    conn = None
    try:
        conn = get_db_connection()
        if conn is None:
            return pd.DataFrame()
        
        logger.info(f"Executando consulta: {query[:100]}...")
        df = pd.read_sql(query, conn, params=params)
        
        # Converter colunas de data para datetime
        date_cols = [col for col in df.columns if 'data' in col.lower() or 'mes' in col.lower()]
        for col in date_cols:
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                continue
            try:
                df[col] = pd.to_datetime(df[col])
            except:
                continue
                
        return df
    except Exception as e:
        st.error(f"Erro ao executar consulta: {e}")
        logger.error(f"Erro na consulta: {str(e)}")
        return pd.DataFrame()

def format_milhar(n: int) -> str:
    """Formata número com separador de milhar."""
    return f"{n:,.0f}".replace(",", ".")

# =============================================
# CARREGAMENTO DE DADOS
# =============================================

@timing_decorator
def load_base_data():
    """Carrega os dados base para filtros e análises"""
    data = {}
    
    # Dados de migração
    data['migracao'] = fetch_data("""
        SELECT data_migracao, COUNT(*) AS total_migracoes
        FROM ccee_parcela_carga_consumo_2025
        WHERE data_migracao IS NOT NULL
        GROUP BY data_migracao
        ORDER BY data_migracao
    """)
    
    # Dados de consumo por perfil
    data['consumo'] = fetch_data("""
        SELECT 
            mes_referencia, 
            ramo_atividade, 
            SUM(consumo_total) as consumo_total,
            CASE 
                WHEN sigla_perfil_agente LIKE 'V%' THEN 'Varejista'
                WHEN sigla_perfil_agente LIKE 'L%' THEN 'Consumidor Livre'
                WHEN sigla_perfil_agente LIKE 'E%' THEN 'Consumidor Especial'
                ELSE 'Outros'
            END as tipo_consumidor
        FROM ccee_parcela_carga_consumo_2025
        WHERE ramo_atividade IS NOT NULL
        GROUP BY mes_referencia, ramo_atividade, tipo_consumidor
    """)
    
    # Lista de CNAEs disponíveis
    data['cnaes'] = fetch_data("""
        SELECT DISTINCT ramo_atividade 
        FROM ccee_parcela_carga_consumo_2025 
        WHERE ramo_atividade IS NOT NULL
        ORDER BY ramo_atividade
    """)['ramo_atividade'].tolist()
    
    return data

# =============================================
# INTERFACE PRINCIPAL
# =============================================

def main():
    tempo_inicio = time.time()
    
    st.title("👷‍♂️ Análise Avançada por Perfil")
    st.caption(f"Dados atualizados em: {agora}")
    
    # Carrega dados base
    data = load_base_data()
    
    # =================== SIDEBAR COM FILTROS ====================
    with st.sidebar:
        st.header("🔍 Filtros Avançados")
        
        # Filtro de tipo de consumidor
        tipo_consumidor = st.selectbox(
            "Tipo de Consumidor",
            ["Varejista", "Consumidor Livre", "Consumidor Especial"],
            index=0
        )
        
        # Filtro de CNAE
        cnae_selecionado = st.selectbox(
            "Ramo de Atividade (CNAE)",
            ["Todos"] + sorted(data['cnaes']),
            index=0
        )
        
        # Filtro de período
        data_inicio, data_fim = st.date_input(
            "Período de Análise",
            value=[datetime.now() - timedelta(days=365), datetime.now()],
            format="DD/MM/YYYY"
        )
        
        # Validação do intervalo de datas
        if data_inicio > data_fim:
            st.error("A data de início não pode ser posterior à data final")
            st.stop()
            
        # Converter para datetime64[ns] para comparação com Timestamp
        data_inicio_dt = pd.to_datetime(data_inicio).tz_localize(None)
        data_fim_dt = pd.to_datetime(data_fim).tz_localize(None)
        
        st.markdown("---")
        st.markdown("**Configurações de Exibição**")
        show_details = st.checkbox("Mostrar detalhes completos", value=True)
    
    # =================== VISUALIZAÇÕES ====================
    
    # Gráfico de migração temporal
    st.subheader("📈 Evolução da Migração para o Mercado Livre")
    
    if not data['migracao'].empty:
        # Converter coluna de data para datetime se necessário
        if not pd.api.types.is_datetime64_any_dtype(data['migracao']['data_migracao']):
            data['migracao']['data_migracao'] = pd.to_datetime(data['migracao']['data_migracao'])
        
        # Aplicar filtro de data
        mask = (data['migracao']['data_migracao'].dt.date >= data_inicio) & \
               (data['migracao']['data_migracao'].dt.date <= data_fim)
        
        df_migracao_filtrado = data['migracao'][mask]
        
        if not df_migracao_filtrado.empty:
            fig_migracao = px.bar(
                df_migracao_filtrado, 
                x="data_migracao", 
                y="total_migracoes",
                labels={"data_migracao": "Data", "total_migracoes": "Número de Migrações"},
                title="Total de Migrações por Data",
                color_discrete_sequence=['#667eea']
            )
            st.plotly_chart(fig_migracao, use_container_width=True)
        else:
            st.warning("Nenhum dado de migração encontrado para o período selecionado")
    else:
        st.warning("Nenhum dado de migração disponível")
        
    # Gráfico de consumo por perfil e CNAE
    st.subheader(f"⚡ Volume de Energia por {tipo_consumidor}")

    if not data['consumo'].empty:
        # Converter coluna de data para datetime - CORREÇÃO AQUI
        if not pd.api.types.is_datetime64_any_dtype(data['consumo']['mes_referencia']):
            try:
                # Supondo que mes_referencia está no formato YYYYMM (ex: 202501 para janeiro/2025)
                data['consumo']['mes_referencia'] = pd.to_datetime(
                    data['consumo']['mes_referencia'], 
                    format='%Y%m'
                )
            except Exception as e:
                st.error(f"Erro ao converter datas: {str(e)}")
                logger.error(f"Erro na conversão de datas: {str(e)}")
        
        # Aplicar filtros
        mask = (data['consumo']['tipo_consumidor'] == tipo_consumidor) & \
               (data['consumo']['mes_referencia'].dt.date >= data_inicio) & \
               (data['consumo']['mes_referencia'].dt.date <= data_fim)
        
        df_consumo_filtrado = data['consumo'][mask]
        
        if cnae_selecionado != "Todos":
            df_consumo_filtrado = df_consumo_filtrado[
                df_consumo_filtrado['ramo_atividade'] == cnae_selecionado
            ]
        
        if not df_consumo_filtrado.empty:
            fig_consumo = px.bar(
                df_consumo_filtrado,
                x="mes_referencia",
                y="consumo_total",
               
                labels={"mes_referencia": "Mês", "consumo_total": "MWh", "ramo_atividade": "Ramo de Atividade"}
                
            )
            fig_consumo.update_xaxes(
                tickformat="%b/%Y"  # Formato Mês/Ano (ex: Jan/2025)
            )
            st.plotly_chart(fig_consumo, use_container_width=True)
        else:
            st.warning(f"Nenhum dado de consumo disponível para {tipo_consumidor} no período selecionado")
    else:
        st.warning("Nenhum dado de consumo disponível")
        
    # Dados detalhados dos agentes
    if show_details:
        st.subheader("📋 Detalhes dos Agentes")
        
        # Consulta dinâmica baseada nos filtros
        perfil_sigla_map = {
            "Varejista": "V",
            "Consumidor Livre": "L",
            "Consumidor Especial": "E"
        }
        perfil_sigla = perfil_sigla_map[tipo_consumidor]

        query_agentes = f"""
            SELECT 
                nome_empresarial,
                cnpj,
                sigla_perfil_agente,
                ramo_atividade,
                estado_uf,
                municipio,
                consumo_total
            FROM view_consumidores_especiais
            WHERE sigla_perfil_agente = '{perfil_sigla}'
        """

        if cnae_selecionado != "Todos":
            query_agentes += f" AND ramo_atividade = '{cnae_selecionado}'"

        query_agentes += """
            ORDER BY consumo_total DESC
        """
        df_agentes = fetch_data(query_agentes)
        
        if not df_agentes.empty:
            st.dataframe(
                df_agentes,
                column_config={
                    "consumo_total": st.column_config.NumberColumn("Consumo Total (MWh)", format="%.2f"),
                    "qtd_migracoes": st.column_config.NumberColumn("Qtd. Migrações", format="%d")
                },
                use_container_width=True,
                hide_index=True
            )
            
            # Botão para download
            csv = df_agentes.to_csv(index=False).encode("utf-8")
            st.download_button(
                label="📥 Baixar tabela como CSV",
                data=csv,
                file_name=f"detalhes_{tipo_consumidor.lower().replace(' ', '_')}.csv",
                mime="text/csv"
            )
        else:
            st.warning("Nenhum agente encontrado com os filtros selecionados.")

    # --- Rodapé ---
    st.markdown("---")
    st.markdown("""
    <div style="text-align: center; color: gray;">
        <strong>Fonte de Dados</strong><br>
        <ul style="list-style:none; padding:0;">
            <li><a href="https://www.ccee.org.br/portal/mercado-livre-de-energia" target="_blank">CCEE - Câmara de Comercialização de Energia Elétrica</a></li>
        </ul>
        <small>📅 Dados atualizados periodicamente</small>
        
    </div>
    """, unsafe_allow_html=True)

    # Tempo total de execução
    tempo_total = time.time() - tempo_inicio
    st.success(f"⏱️ Tempo total de carregamento: {tempo_total:.2f} segundos")

if __name__ == "__main__":
    main()