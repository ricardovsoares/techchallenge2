-- 1. Visualizar o proço médio mensal e o volume de negociação
SELECT 
    ticker, 
    month, 
    AVG(vlr_fechamento) as preco_medio_mensal,
    SUM(qtd_volume) as volume_total_mensal
FROM "b3_analytics"."refined"
GROUP BY ticker, month
ORDER BY month DESC, preco_medio_mensal DESC;


-- 2. Visualizar os 10 primeiros registros da tabela de quotes diárias
SELECT *
FROM b3_analytics.b3_refined_quotes
LIMIT 10;

