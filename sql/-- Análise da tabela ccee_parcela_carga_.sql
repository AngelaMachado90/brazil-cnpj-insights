-- Análise da tabela ccee_parcela_carga_consumo_2025:
-- Contagem total de registros
cnpj_receita=# SELECT COUNT(*) AS total_registros FROM ccee_parcela_carga_consumo_2025;
 total_registros 
-----------------
          127773

-- Período coberto pelos dados
SELECT 
    MIN(TO_DATE(mes_referencia, 'YYYY-MM')) AS data_inicio,
    MAX(TO_DATE(mes_referencia, 'YYYY-MM')) AS data_fim
FROM ccee_parcela_carga_consumo_2025;

data_inicio  |   data_fim   
--------------+--------------
 202501-01-01 | 202503-01-01
(1 row)


-- Distribuição por estado
SELECT 
    estado_uf,
    COUNT(*) AS quantidade,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentual
FROM ccee_parcela_carga_consumo_2025
GROUP BY estado_uf
ORDER BY quantidade DESC;

 estado_uf | quantidade | percentual 
-----------+------------+------------
 SP        |      25963 |      20.32
 SP        |      12980 |      10.16
 RS        |       8626 |       6.75
 PR        |       7788 |       6.10
 MG        |       6718 |       5.26
 RJ        |       6696 |       5.24
 SC        |       5082 |       3.98
 RS        |       4308 |       3.37
 PR        |       3886 |       3.04
 BA        |       3882 |       3.04
 MG        |       3366 |       2.63
 RJ        |       3348 |       2.62
 PE        |       2584 |       2.02
 SC        |       2547 |       1.99
 GO        |       2219 |       1.74
 CE        |       2121 |       1.66
 ES        |       1976 |       1.55
 BA        |       1949 |       1.53
 MT        |       1713 |       1.34
 PA        |       1680 |       1.31
 PE        |       1291 |       1.01
 MS        |       1179 |       0.92
 GO        |       1111 |       0.87
 CE        |       1063 |       0.83
 MA        |       1006 |       0.79
 ES        |        991 |       0.78
 RN        |        899 |       0.70
 AM        |        884 |       0.69
 MT        |        855 |       0.67
 PA        |        838 |       0.66
 DF        |        831 |       0.65

--Distribuição por estado
SELECT 
    nome_empresarial,
    cnpj_carga,
    SUM(consumo_total) AS consumo_total_acumulado
FROM ccee_parcela_carga_consumo_2025
GROUP BY nome_empresarial, cnpj_carga
ORDER BY consumo_total_acumulado DESC
LIMIT 10;

-- Análise da tabela ccee_agente_perfil:
-- Perfis de agentes mais comuns
SELECT 
    sigla_perfil_agente,
    COUNT(*) AS quantidade,
    ROUND(AVG(valor_trc), 2) AS media_trc,
    ROUND(AVG(valor_tggc), 2) AS media_tggc
FROM ccee_agente_perfil
GROUP BY sigla_perfil_agente
ORDER BY quantidade DESC;

     sigla_perfil_agente       | quantidade | media_trc  | media_tggc 
--------------------------------+------------+------------+------------
 SAO JOAO I5                    |          1 |            |       7.79
 VENTOS I I5                    |          1 |            |       7.17
 INCOMFRAL                      |          1 |     673.02 |           
 BON GELO CL 514                |          1 |     380.88 |           
 AGROAVES LTDA CL 514           |          1 |     196.68 |           
 ARATU MINERACAO                |          1 |     112.70 |           
 FUGA E PANORAMA CL 514         |          1 |     139.04 |           
 DUBAI GRANITOS CL 514          |          1 |       5.56 |           
 CASA DE SAUDE DE SANTOS CL 514 |          1 |     488.87 |           
 BOLSON CL 514                  |          1 |     349.94 |           
 PIRACANJUBA GO                 |          1 |    4940.60 |           
 FELISBINO CL 514               |          1 |     310.60 |           
 PLUMA CASSILANDIA              |          1 |     417.50 |           
 UMAFLEX CL 514                 |          1 |     147.29 |           
 FLAMINGO CL 514                |          1 |     128.75 |           
 LOREAL - PROCOSA               |          1 |    1927.94 |           
 SGVIDROS-CBOM                  |          1 |    3172.05 |           
 JP FARMA SUL                   |          1 |     209.81 |           
 COUROS NOBRE CL                |          1 |     170.32 |           
 NEW TURTLE CE                  |          1 |     223.60 |           
 SUPERMERCADOS CASAGRACL 514 23 |          1 |     180.03 |           
 IND RIO DESERTO CL             |          1 |    2232.74 |           
 CORINA CL 514                  |          1 |     255.49 |           
 BRAIR                          |          1 |     591.18 |           
 ARTECOLA QUIMICA               |          1 |     140.33 |           
 TRG PINTURAS TECNICAS          |          1 |     399.62 |           
 NUTRITION FOODS                |          1 |     209.66 |           
 PCH SAO LUIS I5                |          1 |            |       0.00
 SIGNODE                        |          1 |    1212.26 |           
 ELIC - CONSORCIO PASSO R       |          1 |            |       0.00
 TEMASA CL                      |          1 |     930.52 |           

-- Empresas que migraram nos últimos 5 anos (com data de migração explícita)
SELECT 
    p.nome_empresarial,
    p.cnpj_carga,
    p.data_migracao,
    p.sigla_perfil_agente AS perfil_anterior,
    p.sigla_perfil_agente_conectado AS perfil_atual,
    p.consumo_total,
    e.razao_social,
    e.porte
FROM ccee_parcela_carga_consumo_2025 p
LEFT JOIN rfb_empresas e ON p.cnpj_carga = e.cnpj_basico
WHERE p.data_migracao >= (CURRENT_DATE - INTERVAL '5 years')
ORDER BY p.data_migracao DESC
LIMIT 10;
-- Consulta esta pesada, pode demorar um pouco para retornar os resultados
-- to do : melhorar a performance desta consulta, talvez criar um índice na coluna data_migracao

-- Evolução mensal de migrações

cnpj_receita=# -- Evolução mensal de migrações
SELECT 
    TO_CHAR(TO_DATE(mes_referencia, 'YYYY-MM'), 'YYYY-MM') AS mes,
    COUNT(DISTINCT cnpj_carga) AS empresas_migrando,
    SUM(consumo_total) AS consumo_total_migrado
FROM ccee_parcela_carga_consumo_2025
WHERE data_migracao IS NOT NULL
GROUP BY mes
ORDER BY mes;
    mes    | empresas_migrando | consumo_total_migrado 
-----------+-------------------+-----------------------
 202501-01 |             33709 |       54155351.343387
 202502-01 |             33887 |       52451058.955856
 202503-01 |             34027 |       55530245.700602
(3 rows)

cnpj_receita=# -- Comparação entre varejistas e consumidores
SELECT 
    v.sigla_perfil_agente,
    v.nome_empresarial,
    v.estado_uf_carga,
    v.consumo_total AS consumo_varejista,
    p.consumo_total AS consumo_parcela,
    (v.consumo_total - p.consumo_total) AS diferenca
FROM ccee_varejista_consumidor_2025 v
JOIN ccee_parcela_carga_consumo_2025 p 
    ON v.cnpj_carga = p.cnpj_carga 
    AND v.mes_referencia = p.mes_referencia
LIMIT 10;
ERROR:  column v.cnpj_carga does not exist
LINE 10:     ON v.cnpj_carga = p.cnpj_carga 
                ^
HINT:  Perhaps you meant to reference the column "p.cnpj_carga".

cnpj_receita=# -- Análise de perfil de agentes com lista de perfil
SELECT 
    a.nome_empresarial,
    a.cnpj,
    a.sigla_perfil_agente,
    l.classe_perfil_agente,
    l.status_perfil,
    l.categoria_agente
FROM ccee_agente_perfil a
JOIN ccee_lista_perfil_2025 l ON a.cnpj = l.cnpj
ORDER BY a.nome_empresarial
LIMIT 10;
               nome_empresarial               |      cnpj      |  sigla_perfil_agente  | classe_perfil_agente | status_perfil | categoria_agente 
----------------------------------------------+----------------+-----------------------+----------------------+---------------+------------------
 1001 INDUSTRIA DE ARTEFATOS DE BORRACHA LTDA | 61508537000151 | 1001 INDUSTRIA CL 514 | Consumidor Especial  | ATIVO         | Comercialização
 1001 INDUSTRIA DE ARTEFATOS DE BORRACHA LTDA | 61508537000151 | 1001 INDUSTRIA CL 514 | Consumidor Especial  | ATIVO         | Comercialização
 1001 INDUSTRIA DE ARTEFATOS DE BORRACHA LTDA | 61508537000151 | 1001 INDUSTRIA CL 514 | Consumidor Livre     | ATIVO         | Comercialização
 1001 INDUSTRIA DE ARTEFATOS DE BORRACHA LTDA | 61508537000151 | 1001 INDUSTRIA CL 514 | Consumidor Livre     | ATIVO         | Comercialização
 1001 INDUSTRIA DE ARTEFATOS DE BORRACHA LTDA | 61508537000151 | 1001 INDUSTRIA CL 514 | Consumidor Livre     | ATIVO         | Comercialização
 1001 INDUSTRIA DE ARTEFATOS DE BORRACHA LTDA | 61508537000151 | 1001 INDUSTRIA CL 514 | Consumidor Especial  | ATIVO         | Comercialização
 101 BRASIL INDUSTRIA DE BEBIDAS LTDA         | 03408722000178 | 101 DO BRASIL CL 514  | Consumidor Especial  | ATIVO         | Comercialização
 101 BRASIL INDUSTRIA DE BEBIDAS LTDA         | 03408722000178 | 101 DO BRASIL CL 514  | Consumidor Livre     | ATIVO         | Comercialização
 101 BRASIL INDUSTRIA DE BEBIDAS LTDA         | 03408722000178 | 101 DO BRASIL CL 514  | Consumidor Especial  | ATIVO         | Comercialização
 101 BRASIL INDUSTRIA DE BEBIDAS LTDA         | 03408722000178 | 101 DO BRASIL CL 514  | Consumidor Livre     | ATIVO         | Comercialização
(10 rows)

cnpj_receita=# -- Análise por porte de empresa
SELECT 
    e.porte,
    COUNT(DISTINCT p.cnpj_carga) AS empresas_migrando,
    SUM(p.consumo_total) AS consumo_total
FROM ccee_parcela_carga_consumo_2025 p
JOIN rfb_empresas e ON p.cnpj_carga = e.cnpj_basico
WHERE p.data_migracao IS NOT NULL
GROUP BY e.porte;

SELECT
    e.porte,
    es.cnpj_completo as "CNPJ RECEITA",
    es.cnpj_basico as "CNPJ BASE",
    es.nome_fantasia as "NOME FANTASIA",
    es.razao_social as "RAZAO SOCIAL",
    es.cnae_fiscal_principal as "CNAE FISCAL",
    es.cnae_fiscal_secundario as "CNAE SECUNDARIO",
    -- tabela socios
    s.nome_socios as "NOME SOCIOS",
    s.cnpj_socios as "CNPJ SOCIOS",
    s.qualificacao_socios as "QUALIFICACAO SOCIOS",
    -- s.pais_socios as "PAIS SOCIOS",
    s.data_entrada_socios as "DATA ENTRADA SOCIOS",
    s.data_saida_socios as "DATA SAIDA SOCIOS",

    -- dados de contato provenientes da tabela rfb_estabelecimentos
    es.email as "EMAIL",
      -- Telefone 1: concatena DDD1 e TELEFONE1
    CASE 
        WHEN es.DDD1 IS NOT NULL AND es.TELEFONE1 IS NOT NULL THEN CONCAT('(', es.DDD1, ') ', es.TELEFONE1)
        WHEN es.DDD1 IS NULL AND es.TELEFONE1 IS NOT NULL THEN es.TELEFONE1
        WHEN es.DDD1 IS NOT NULL AND es.TELEFONE1 IS NULL THEN CONCAT('(', es.DDD1, ')')
        ELSE NULL
    END AS TELEFONE01,
    
    -- Telefone 2: concatena DDD2 e TELEFONE2
    CASE 
        WHEN es.DDD2 IS NOT NULL AND es.TELEFONE2 IS NOT NULL THEN CONCAT('(', es.DDD2, ') ', es.TELEFONE2)
        WHEN es.DDD2 IS NULL AND es.TELEFONE2 IS NOT NULL THEN es.TELEFONE2
        WHEN es.DDD2 IS NOT NULL AND es.TELEFONE2 IS NULL THEN CONCAT('(', es.DDD2, ')')
        ELSE NULL
    END AS TELEFONE02,
    
    -- Fax: concatena DDDFAX e FAX
    CASE 
        WHEN es.DDDFAX IS NOT NULL AND es.FAX IS NOT NULL THEN CONCAT('(', es.DDDFAX, ') ', es.FAX)
        WHEN es.DDDFAX IS NULL AND es.FAX IS NOT NULL THEN es.FAX
        WHEN es.DDDFAX IS NOT NULL AND es.FAX IS NULL THEN CONCAT('(', es.DDDFAX, ')')
        ELSE NULL
    END AS TELEFONE03,

    COUNT(DISTINCT p.cnpj_carga) AS empresas_migrando,
    SUM(p.consumo_total) AS consumo_total,
    -- Dados dos Socios

FROM ccee_parcela_carga_consumo_2025 p

JOIN rfb_empresas e ON p.cnpj_carga = e.cnpj_basico
JOIN rfb_estabelecimentos es ON p.cnpj_carga = es.cnpj_basico
JOIN rfb_socios s ON p.cnpj_carga = s.cnpj_basico

WHERE p.data_migracao IS NOT NULL
      AND p.data_migracao >= (CURRENT_DATE - INTERVAL '5 years')
GROUP BY e.porte, es.cnpj_completo, es.cnpj_basico, es.nome_fantasia, 
         es.razao_social, es.cnae_fiscal_principal,
         es.cnae_fiscal_secundario, s.nome_socios, s.cnpj_socios, 
         s.qualificacao_socios, s.data_entrada_socios,
         s.data_saida_socios, es.email, es.DDD1, es.TELEFONE1, 
         es.DDD2, es.TELEFONE2, es.DDDFAX, es.FAX
ORDER BY e.porte, consumo_total DESC
LIMIT 10;