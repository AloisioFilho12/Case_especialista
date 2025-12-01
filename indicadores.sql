-- SQLite
CREATE TABLE IF NOT EXISTS indicadores AS
WITH TotalGeral AS (
    
    SELECT COUNT(id) AS total
    FROM usuarios
)
-- INÍCIO DA QUERY PRINCIPAL QUE CRIA A TABELA (UNION ALL)
SELECT 
    'Maior 40 anos (Geral)' AS nm_indicador,
    -- Usa a CTE no denominador (proporção de usuários > 40 no TOTAL)
    (SUM(CASE WHEN idade > 40 THEN 1.0 ELSE 0.0 END) / (SELECT total FROM TotalGeral)) AS vlr_indicador
FROM 
    usuarios

UNION ALL

SELECT 
    'Sao clientes mastercard' AS nm_indicador,
    -- Usa a CTE no denominador (proporção de usuários > 40 no TOTAL)
    (SUM(CASE WHEN cartao = 'Mastercard' THEN 1.0 ELSE 0.0 END) / (SELECT total FROM TotalGeral)) AS vlr_indicador
FROM 
    usuarios

UNION ALL

SELECT 
    'Cartao em moeda diferente de USD' AS nm_indicador,
    -- Usa a CTE no denominador (proporção de usuários > 40 no TOTAL)
    (SUM(CASE WHEN moeda != 'USD' THEN 1.0 ELSE 0.0 END) / (SELECT total FROM TotalGeral)) AS vlr_indicador
FROM 
    usuarios

UNION ALL

SELECT 
    'Estudaram em Havard' AS nm_indicador,
    -- Usa a CTE no denominador (proporção de usuários > 40 no TOTAL)
    (SUM(CASE WHEN universidade = 'Harvard University' THEN 1.0 ELSE 0.0 END) / (SELECT total FROM TotalGeral)) AS vlr_indicador

FROM 
    usuarios

UNION ALL

-- INDICADOR POR ESTADO
SELECT 
    'Moram em ' || estado AS nm_indicador,
    -- Usa a CTE no denominador (proporção do estado no TOTAL)
    (COUNT(id) * 1.0) / (SELECT total FROM TotalGeral) AS vlr_indicador
FROM 
    usuarios
GROUP BY 
    estado

UNION ALL

-- INDICADOR POR UNIVERSIDADE
SELECT 
    'Trabalha em ' || universidade AS nm_indicador,
    -- Usa a CTE no denominador (proporção da universidade no TOTAL)
    (COUNT(id) * 1.0) / (SELECT total FROM TotalGeral) AS vlr_indicador
FROM 
    usuarios
GROUP BY 
    universidade;