# 📊 Documentação do Banco de Dados  

## 1. Visão Geral  
**Banco de Dados:** `cnpj_receita`  
**Tecnologia:** [MySQL/PostgreSQL/MongoDB/etc.]  
**Propósito:** [Descrição breve do objetivo do banco, ex: "Armazenar dados de usuários e transações para o sistema XYZ"].  

### Diagrama de Entidade-Relacionamento (ER)  
```mermaid
erDiagram
    CLIENTES ||--o{ PEDIDOS : "1:N"
    PEDIDOS ||--|{ ITENS : "1:N"
    PRODUTOS }o--|| ITENS : "referencia"


### Consultas 
```mermaid
Exemplo: Verificar se há CNPJs + sócios duplicados
cnpj_receita=# -- Exemplo: Verificar se há CNPJs + sócios duplicados
SELECT cnpj_basico, cnpj_cpf, COUNT(*)
FROM rfb_socios
GROUP BY cnpj_basico, cnpj_cpf
HAVING COUNT(*) > 1;

-- Opção 1: Usando uma subconsulta
SELECT *
FROM rfb_socios
WHERE (cnpj_basico, cnpj_cpf) IN (
    SELECT cnpj_basico, cnpj_cpf
    FROM rfb_socios
    GROUP BY cnpj_basico, cnpj_cpf
    HAVING COUNT(*) > 1
)
ORDER BY cnpj_basico, cnpj_cpf;

-- Opção 2: Usando JOIN (mais eficiente em bancos grandes)
SELECT a.*
FROM rfb_socios a
JOIN (
    SELECT cnpj_basico, cnpj_cpf
    FROM rfb_socios
    GROUP BY cnpj_basico, cnpj_cpf
    HAVING COUNT(*) > 1
) b ON a.cnpj_basico = b.cnpj_basico AND a.cnpj_cpf = b.cnpj_cpf
ORDER BY a.cnpj_basico, a.cnpj_cpf;


-- Exemplo: Verificar se há CNPJs + sócios duplicados (com CNPJ completo)
SELECT cnpj_basico, cnpj_cpf, COUNT(*)
FROM rfb_socios
WHERE cnpj_basico IS NOT NULL AND cnpj_cpf IS NOT NULL
GROUP BY cnpj_basico, cnpj_cpf
HAVING COUNT(*) > 1
ORDER BY cnpj_basico, cnpj_cpf;

 id_socio | cnpj_basico | tipo_socio | nome_razao_social | cnpj_cpf | qualificacao | data_entrada_sociedade | pais | cpf_representante_legal | nome_representante_legal | qualificacao_representante_legal | faixa_etaria 
----------+-------------+------------+-------------------+----------+--------------+------------------------+------+-------------------------+--------------------------+----------------------------------+--------------
 22969851 | 00080044    | 2          |                   | 061101   | 16           | 2005-09-12             |      | 000000                  |                          | 00                               | 
 12474700 | 00080044    | 2          |                   | 061101   | 16           | 2005-09-12             |      | 000000                  |                          | 00                               | 
 35716173 | 00080044    | 2          |                   | 061101   | 16           | 2005-09-12             |      | 000000                  |                          | 00                               | 
 29343012 | 00080044    | 2          |                   | 061101   | 16           | 2005-09-12             |      | 000000                  |                          | 00                               | 
 53007178 | 00080044    | 2          |                   | 061101   | 16           | 2005-09-12             |      | 000000                  |                          | 00                               | 
(5 rows)


-- Exemplo: Consultar os últimos 100 registros da tabela ccee_parcela_carga_consumo_2025
-- A consulta retorna os campos mes_referencia, cod_per_agente, sigla_peril_agente, nome_empresarial,
-- cod_parcela_carga, sigla_parcela_carga, cnpj_carga, cidade, qtd_parcela_carga,
-- consumo_total e data_importacao
-- A consulta ordena os resultados pela coluna mes_referencia em ordem decrescente
-- e limita o resultado a 100 registros
SELECT 
    mes_referencia,
    cod_perf_agente,
    sigla_perfil_agente,
    nome_empresarial,
    cod_parcela_carga,
    sigla_parcela_carga,
    cnpj_carga,
    cidade,
    --qtd_parcela_carga,
    consumo_total,
    data_importacao
FROM ccee_parcela_carga_consumo_2025
ORDER BY mes_referencia DESC
LIMIT 100;
