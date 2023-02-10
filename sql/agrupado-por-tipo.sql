SELECT
Tipo,
SUM(ValorContratado) as ValorContratado
FROM TabelaLicitacao
GROUP BY Tipo
ORDER BY ValorContratado DESC