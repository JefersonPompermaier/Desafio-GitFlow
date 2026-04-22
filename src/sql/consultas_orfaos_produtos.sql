SELECT COUNT(DISTINCT i.id_produto)
FROM itens_pedido i
LEFT JOIN produtos p ON i.id_produto = p.id_produto
WHERE p.id_produto IS NULL;
