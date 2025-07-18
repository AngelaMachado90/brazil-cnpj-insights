---- consulta sobre a quantidade de linhas inseridas na tabela rfb_socios
-- e o progresso em relação ao total de 25.938.492 linhas esperadas
-- A consulta retorna o total de linhas, total de inseridos, o que falta e o progresso em porcentagem
-- A formatação dos números é feita para substituir a vírgula por ponto
-- e para exibir os números com separadores de milhar
-- A consulta utiliza a função to_char para formatar os números
-- e a função replace para substituir a vírgula por ponto
-- A consulta também calcula a porcentagem de progresso
-- e formata o resultado como uma string com o símbolo de porcentagem
WITH contagem AS (
    SELECT COUNT(*) AS qtd_inseridos FROM rfb_socios
)
SELECT 
    replace(to_char(25938492, 'FM999G999G999'), ',', '.') AS total_linhas,
    replace(to_char(qtd_inseridos, 'FM999G999G999'), ',', '.') AS total_inseridos,
    replace(to_char(25938492 - qtd_inseridos, 'FM999G999G999'), ',', '.') AS faltam,
    ROUND((qtd_inseridos::numeric / 25938492) * 100, 2) || '%' AS progresso
FROM contagem;