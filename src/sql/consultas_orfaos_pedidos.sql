SELECT COUNT(DISTINCT i.id_pedido)
FROM itens_pedido i
LEFT JOIN pedidos p ON i.id_pedido = p.id_pedido
WHERE p.id_pedido IS NULL;
