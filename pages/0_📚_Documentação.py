import streamlit as st
import graphviz
import psycopg2
from sqlalchemy import create_engine
from sqlalchemy import inspect
from datetime import datetime
import pandas as pd
from contextlib import contextmanager

# Configuração da página
st.set_page_config(
    page_title="Documentação Projeto Emewe Mailling - RFB e CCEE",
    page_icon="📚",
    layout="wide"
)

# Gerenciador de contexto para conexões
@contextmanager
def db_connection():
    conn = None
    try:
        conn = psycopg2.connect(
            host="emewe-mailling-db",
            database="cnpj_receita",
            user="postgres",
            password="postgres",
            port=5432
        )
        yield conn
    except Exception as e:
        st.error(f"Erro de conexão: {str(e)}")
        raise
    finally:
        if conn:
            conn.close()

# Criando engine SQLAlchemy para uso com pandas
@st.cache_resource
def get_sqlalchemy_engine():
    """Cria engine SQLAlchemy para consultas"""
    return create_engine('postgresql+psycopg2://postgres:postgres@emewe-mailling-db:5432/cnpj_receita')

engine = get_sqlalchemy_engine()

# Função para obter metadados do banco
@st.cache_data(ttl=3600)  # Cache por 1 hora
def get_database_metadata():
    inspector = inspect(engine)
    
    # Obter todas as tabelas
    tables = inspector.get_table_names()
    schemas = inspector.get_schema_names()
    
    # Obter metadados detalhados
    metadata = []
    for schema in schemas:
        if schema in ('pg_catalog', 'information_schema'):
            continue
            
        for table in inspector.get_table_names(schema=schema):
            for column in inspector.get_columns(table, schema=schema):
                metadata.append({
                    'schema': schema,
                    'table': table,
                    'column': column['name'],
                    'type': str(column['type']),
                    'nullable': column['nullable'],
                    'default': str(column['default']) if column['default'] else None,
                    'pk': False,
                    'fk': False
                })
    
    # Identificar chaves primárias
    for schema in schemas:
        for table in inspector.get_table_names(schema=schema):
            try:
                pks = inspector.get_pk_constraint(table, schema=schema)
                for col in pks['constrained_columns']:
                    for item in metadata:
                        if item['schema'] == schema and item['table'] == table and item['column'] == col:
                            item['pk'] = True
            except Exception:
                continue
    
    # Identificar chaves estrangeiras
    for schema in schemas:
        for table in inspector.get_table_names(schema=schema):
            try:
                fks = inspector.get_foreign_keys(table, schema=schema)
                for fk in fks:
                    for col in fk['constrained_columns']:
                        for item in metadata:
                            if item['schema'] == schema and item['table'] == table and item['column'] == col:
                                item['fk'] = True
                                item['fk_ref'] = f"{fk['referred_schema']}.{fk['referred_table']}.{fk['referred_columns'][0]}"
            except Exception:
                continue
    
    return pd.DataFrame(metadata)

# Funções auxiliares para os cards
@st.cache_data
def fetch_data(query):
    return pd.read_sql(query, engine)

def get_current_time():
    return datetime.now().strftime("%d/%m/%Y %H:%M")

def format_milhar(value):
    return f"{value:,.0f}".replace(",", ".")

# Menu de navegação
st.sidebar.title("Navegação")
page = st.sidebar.radio("Selecione a página:", 
                       ["Dashboard Principal", "Dicionário de Dados", "Diagrama Entidade-Relacionamento", 
                        "Consultas do Banco", "Documentação Técnica"])

# Página principal do dashboard
if page == "Dashboard Principal":
    st.title("Dashboard de Análise de Dados CNPJ e CCEE")
    st.write("Bem-vindo ao dashboard de análise integrada de dados da Receita Federal e CCEE.")
    
    # Carregar metadados
    try:
        metadata = get_database_metadata()
        
        # --- Cards de métricas ---
        st.markdown("""
        <style>
        .metric-card {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            border-radius: 10px;
            padding: 15px;
            color: white;
            margin-bottom: 20px;
            box-shadow: 0 4px 8px rgba(0,0,0,0.1);
        }
        .metric-title {
            font-size: 14px;
            font-weight: 500;
            margin-bottom: 5px;
            opacity: 0.8;
        }
        .metric-value {
            font-size: 24px;
            font-weight: bold;
        }
        </style>
        """, unsafe_allow_html=True)
        
        # Buscar dados para os cards
        total_tabelas = metadata['table'].nunique()
        total_colunas = len(metadata)
        agora = datetime.now().strftime("%d/%m/%Y %H:%M")
        
        # Seção de métricas
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-title">Total de Tabelas</div>
                <div class="metric-value">{total_tabelas}</div>
            </div>
            """, unsafe_allow_html=True)
        
        with col2:
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-title">Total de Colunas</div>
                <div class="metric-value">{total_colunas}</div>
            </div>
            """, unsafe_allow_html=True)
        
        with col3:
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-title">Última Atualização</div>
                <div class="metric-value" style="font-size:18px;">{agora}</div>
            </div>
            """, unsafe_allow_html=True)
        
        # Visualização rápida das tabelas
        st.subheader("Visão Geral das Tabelas")
        table_counts = metadata.groupby(['schema', 'table']).size().reset_index(name='colunas')
        st.dataframe(table_counts, hide_index=True)
        
    except Exception as e:
        st.error(f"Erro ao conectar ao banco de dados: {str(e)}")

# Página do dicionário de dados
elif page == "Dicionário de Dados":
    st.title("📚 Dicionário de Dados")
    st.write("Documentação completa das tabelas e campos do banco de dados.")
    
    try:
        # Carregar metadados
        metadata = get_database_metadata()
        
        # Filtrar por schema
        schemas = metadata['schema'].unique()
        selected_schema = st.selectbox("Selecione o schema:", schemas)
        
        # Filtrar tabelas do schema selecionado
        schema_tables = metadata[metadata['schema'] == selected_schema]['table'].unique()
        
        if len(schema_tables) == 0:
            st.warning(f"Nenhuma tabela encontrada no schema {selected_schema}")
        else:
            # Abas para cada tabela
            tabs = st.tabs([f"📌 {table}" for table in schema_tables])
            
            for i, table in enumerate(schema_tables):
                with tabs[i]:
                    table_metadata = metadata[(metadata['schema'] == selected_schema) & 
                                            (metadata['table'] == table)]
                    
                    # Exibir informações da tabela
                    st.subheader(f"Tabela: {selected_schema}.{table}")
                    
                    # Mostrar colunas em formato de tabela
                    st.dataframe(
                        table_metadata[['column', 'type', 'nullable', 'pk', 'fk']],
                        column_config={
                            "column": "Coluna",
                            "type": "Tipo de Dado",
                            "nullable": "Permite Nulo?",
                            "pk": "Chave Primária",
                            "fk": "Chave Estrangeira"
                        },
                        hide_index=True,
                        use_container_width=True
                    )
                    
                    # Mostrar detalhes das chaves estrangeiras
                    fks = table_metadata[table_metadata['fk'] == True]
                    if not fks.empty:
                        st.subheader("Relacionamentos")
                        for _, row in fks.iterrows():
                            st.write(f"🔗 {row['column']} → {row['fk_ref']}")
    
    except Exception as e:
        st.error(f"Erro ao acessar metadados: {str(e)}")

# Página do diagrama entidade-relacionamento
elif page == "Diagrama Entidade-Relacionamento":
    st.title("🔷 Diagrama Entidade-Relacionamento (DER)")
    st.write("Visualização do modelo de dados do sistema.")
    
    try:
        # Carregar metadados
        metadata = get_database_metadata()
        
        # Criando o gráfico com Graphviz
        graph = graphviz.Digraph()
        
        # Configurações do gráfico
        graph.attr(rankdir='LR', size='20,15')
        graph.attr('node', shape='rectangle', style='filled', fillcolor='lightblue')
        
        # Agrupar por schema
        schemas = metadata['schema'].unique()
        
        for schema in schemas:
            with graph.subgraph(name=f'cluster_{schema}') as c:
                c.attr(label=schema, color='blue' if schema == 'public' else 'green')
                
                # Filtrar tabelas deste schema
                schema_tables = metadata[metadata['schema'] == schema]['table'].unique()
                
                for table in schema_tables:
                    # Filtrar colunas desta tabela
                    table_data = metadata[(metadata['schema'] == schema) & 
                                        (metadata['table'] == table)]
                    
                    # Criar label do nó
                    label = f"{table}|"
                    
                    # Adicionar colunas PK primeiro
                    pks = table_data[table_data['pk'] == True]
                    for _, row in pks.iterrows():
                        label += f"<b>{row['column']}</b> (PK)|"
                    
                    # Adicionar colunas FK
                    fks = table_data[table_data['fk'] == True]
                    for _, row in fks.iterrows():
                        label += f"{row['column']} (FK)|"
                    
                    # Adicionar outras colunas
                    others = table_data[(table_data['pk'] == False) & (table_data['fk'] == False)]
                    for _, row in others.iterrows():
                        label += f"{row['column']}|"
                    
                    # Adicionar nó ao gráfico
                    c.node(f"{schema}.{table}", f"<{label}>")
        
        # Adicionar relacionamentos
        fks = metadata[metadata['fk'] == True]
        for _, row in fks.iterrows():
            source_table = f"{row['schema']}.{row['table']}"
            target_table = row['fk_ref'].split('.')[1]  # Simplificado
            graph.edge(source_table, target_table, label=row['column'])
        
        # Exibindo o gráfico no Streamlit
        st.graphviz_chart(graph)
        
        # Opção para download da imagem
        svg_bytes = graph.pipe(format='svg')
        st.download_button(
            label="Download do Diagrama (SVG)",
            data=svg_bytes,
            file_name="diagrama_entidade_relacionamento.svg",
            mime="image/svg+xml"
        )
    
    except Exception as e:
        st.error(f"Erro ao gerar diagrama: {str(e)}")

# Página de consultas do banco
elif page == "Consultas do Banco":
    st.title("🔍 Consultas do Banco de Dados")
    st.write("Consultas úteis para explorar a estrutura do banco de dados.")
    
    st.subheader("Consulta Personalizada")
    query = st.text_area("Digite sua consulta SQL:", height=100)
    
    if st.button("Executar Consulta"):
        try:
            with db_connection() as conn:
                result = pd.read_sql(query, conn)
                st.dataframe(result, use_container_width=True)
        except Exception as e:
            st.error(f"Erro na consulta: {str(e)}")
    
    st.divider()
    st.subheader("Consultas Prontas")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("**Listar todas as tabelas**")
        st.code("""
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
        AND table_type = 'BASE TABLE'
        ORDER BY table_schema, table_name;
        """, language='sql')
        
        st.markdown("**Listar colunas de uma tabela**")
        st.code("""
        SELECT column_name, data_type, is_nullable, column_default
        FROM information_schema.columns
        WHERE table_schema = 'public'
        AND table_name = 'nome_da_tabela'
        ORDER BY ordinal_position;
        """, language='sql')
    
    with col2:
        st.markdown("**Consultar chaves primárias**")
        st.code("""
        SELECT 
            tc.table_schema, 
            tc.table_name, 
            kc.column_name
        FROM 
            information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kc
                ON tc.constraint_name = kc.constraint_name
        WHERE 
            tc.constraint_type = 'PRIMARY KEY'
            AND tc.table_schema NOT IN ('pg_catalog', 'information_schema')
        ORDER BY tc.table_schema, tc.table_name;
        """, language='sql')
        
        st.markdown("**Consultar chaves estrangeiras**")
        st.code("""
        SELECT
            tc.table_schema, 
            tc.table_name, 
            kcu.column_name, 
            ccu.table_schema AS foreign_schema,
            ccu.table_name AS foreign_table,
            ccu.column_name AS foreign_column
        FROM 
            information_schema.table_constraints AS tc 
            JOIN information_schema.key_column_usage AS kcu
                ON tc.constraint_name = kcu.constraint_name
            JOIN information_schema.constraint_column_usage AS ccu
                ON ccu.constraint_name = tc.constraint_name
        WHERE 
            tc.constraint_type = 'FOREIGN KEY'
            AND tc.table_schema NOT IN ('pg_catalog', 'information_schema');
        """, language='sql')

# Página de documentação técnica
elif page == "Documentação Técnica":
    st.title("📝 Documentação Técnica")
    
    st.header("1. Objetivo")
    st.write("""
    Este dashboard tem como objetivo fornecer uma visão integrada e atualizada automaticamente 
    dos dados da Receita Federal (CNPJ) e da CCEE (Câmara de Comercialização de Energia Elétrica), 
    permitindo análises conjuntas e cruzamento de informações.
    """)
    
    st.header("2. Arquitetura do Sistema")
    st.graphviz_chart("""
    digraph {
        rankdir=LR;
        node [shape=box];
        
        BancoDados [label="Banco de Dados\nPostgreSQL"];
        Streamlit [label="Dashboard\nStreamlit"];
        Usuario [label="Usuário"];
        
        BancoDados -> Streamlit [label="Consulta Metadados"];
        Streamlit -> Usuario [label="Visualização Interativa"];
        Usuario -> Streamlit [label="Interação"];
        Streamlit -> BancoDados [label="Consultas SQL"];
    }
    """)
    
    st.header("3. Funcionamento da Documentação Automática")
    st.write("""
    O sistema funciona da seguinte maneira:
    1. Conecta-se ao banco de dados PostgreSQL
    2. Extrai os metadados das tabelas, colunas e relacionamentos
    3. Organiza as informações em um formato acessível
    4. Gera visualizações dinâmicas (DER, dicionário de dados)
    5. Atualiza automaticamente quando a estrutura do banco muda
    """)
    
    st.header("4. Como Contribuir")
    st.write("""
    - **Reportar bugs**: Abra uma issue no repositório do projeto
    - **Solicitar features**: Descreva sua sugestão de melhoria
    - **Desenvolvimento**: Fork o repositório e envie um pull request
    """)

# Rodapé
st.markdown("---")
st.markdown(f"""
<div style="text-align: center; color: gray;">
    <strong>Fonte de Dados</strong><br>
    <img src="https://w7.pngwing.com/pngs/173/36/png-transparent-postgresql-logo-computer-software-database-open-source-s-text-head-snout.png" width="150" alt="PlugnGo Logo"><br>
    <ul style="list-style:none; padding:0;">
        <li><a href="https://www.gov.br/receitafederal/" target="_blank">RFB - Receita Federal do Brasil</a></li>
        <li><a href="https://www.ccee.org.br/" target="_blank">CCEE - Câmara de Comercialização de Energia Elétrica</a></li>
        <br>
        <li><a href="https://dadosabertos.ccee.org.br/" target="_blank">Dados Abertos CCEE - Consumo Mensal Perfil Agente</a></li>
        <iframe title="Data viewer" width="750" height="400" src="https://dadosabertos.ccee.org.br/dataset/consumo_mensal_perfil_agente/resource/a26026a8-e270-441f-bb5e-4607ed39d068/view/49b1c637-3886-4696-999c-0c063536d656" frameBorder="0"></iframe>
        <br>
    </ul>
    <ul style="list-style:none; padding:0;">
    <small>📅 Dados atualizados em {datetime.now().strftime("%d/%m/%Y %H:%M")}</small><br>
    <small> Desenvolvido por PlugnGo</small>
    </ul>
</div>
""", unsafe_allow_html=True)