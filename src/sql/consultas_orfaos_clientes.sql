SELECT COUNT(DISTINCT p.id_cliente)
FROM pedidos p
LEFT JOIN clientes c ON p.id_cliente = c.id_cliente
WHERE c.id_cliente IS NULL;
