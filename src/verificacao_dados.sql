--verificacao se existia algum produto nulo em um pedido
select distinct i.id_produto from itens_pedido i left join produtos p on i.id_produto = p.id_produto
where p.id_produto is null;

-- correcao de caracteres (utilizei IA para ajudar na correção)
select distinct categoria from produtos; -- ver categorias incorretas

SELECT -- ver produtos incorretos
    id_produto, 
    nome_produto, 
    categoria, 
    preco
FROM produtos
WHERE id_produto IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY categoria ORDER BY id_produto) = 1;

UPDATE produtos --update de correção
SET 
  nome_produto = REPLACE(REPLACE(REPLACE(REPLACE(nome_produto, 
    'Ã§Ã£', 'çã'), 
    'Ã´', 'ô'), 
    'Ã¡', 'á'), 
    'Ã', 'á'),     
  categoria = REPLACE(REPLACE(REPLACE(REPLACE(categoria, 
    'Ã§Ã£', 'çã'), 
    'Ã´', 'ô'), 
    'Ã¡', 'á'), 
    'Ã', 'á')
WHERE 
  -- Filtro para processar apenas linhas que contenham o erro frequente 'Ã'
  nome_produto LIKE '%Ã%' OR categoria LIKE '%Ã%';


-- verifificacao de preco

select i.id_pedido, p.nome_produto, p.preco as preco_cadastrado, i.preco_unitario as preco_venda, round(i.preco_unitario - p.preco, 2) as diferenca
from itens_pedido i
join produtos p on i.id_produto = p.id_produto
where i.preco_unitario <> p.preco;


-- quantidade de categorias e produtos
SELECT 
    COUNT(DISTINCT categoria) AS total_categorias,
    COUNT(DISTINCT nome_produto) AS total_produtos
FROM produtos;

-- clientes sem pedido

select c.id_cliente, c.nome, c.data_cadastro
from clientes c left join pedidos p on c.id_cliente = p.id_cliente
where p.id_pedido is null; 

-- produtos vendidos sem cadastro

select distinct i.id_produto from itens_pedido i left join produtos p on i.id_produto = p.id_produto
where p.id_produto is null;

-- data de cadastro de cliente posterior ao pedido
-- inicialmente nao funcionou, depois precisei buscar como utilizar a funcao de parsedate

select p.id_pedido, c.nome, c.data_cadastro, p.data_pedido 
from pedidos p
join clientes c on p.id_cliente = c.id_cliente
where p.data_pedido < SAFE.PARSE_DATE('%m/%d/%e', c.data_cadastro);

-- pedidos sem correspondencias na tabela de itens

select i.id_pedido, i.id_produto from itens_pedido i 
left join pedidos p on i.id_pedido = p.id_pedido
where p.id_pedido is null;


-- datas de cadastro posteriores aos pedidos
WITH consulta_tratada AS (
  SELECT 
      p.id_pedido, 
      c.nome, 
      SAFE.PARSE_DATE('%m/%d/%Y', c.data_cadastro) AS data_cadastro_convertida, 
      p.data_pedido,
      -- Cálculo da diferença
      DATE_DIFF(p.data_pedido, SAFE.PARSE_DATE('%m/%d/%Y', c.data_cadastro), DAY) AS dias_reais_diferenca
  FROM pedidos p
  JOIN clientes c ON p.id_cliente = c.id_cliente
)
select * from consulta_tratada
where dias_reais_diferenca < 0
order by dias_reais_diferenca asc;

-- clientes sem pedido
select p.id_pedido, p.id_cliente
from pedidos p
left join clientes c on p.id_cliente = c.id_cliente
where c.id_cliente is null;

-- resolvi ver quantidade de pedidos por estado
select 
    COALESCE(c.estado, 'DESCONHECIDO') AS estado, 
    COUNT(p.id_pedido) AS qtd_pedidos
from pedidos p
left join clientes c on p.id_cliente = c.id_cliente
group by 1;


-- verificação de maior e menor id, data em pedidos

select 
    min(id_pedido) as menor_id, 
    max(id_pedido) as maior_id,
    min(data_pedido) as data_menor_id,
    max(data_pedido) as data_maior_id
from pedidos;

-- após essas contatações foram definidas as seguintes soluções:
--Ajustar tipagem de dados
/* 
  Correção de tipagem nas colunas de data e valor
  Ajustar UTF-8
  Ordenar por ordem cronológica o id_pedido e manter o ID antigo
  Ajustar data de cadastro para o primeiro pedido para os casos inconsistentes
  Inserir clientes desconhecidos na tabela de clientes a partir do ID em pedidos, estado = “DC”, nome = “Desconhecido”
 */

-- verificação de tipo de dados nas tabelas

select table_name, column_name, data_type from dataset_jeferson.INFORMATION_SCHEMA.COLUMNS
where table_name in ('clientes', 'pedidos', 'itens_pedido', 'produtos');

/* tipos de dados encontrados para ajuste
produtos.preco = float64
pedidos.valor_total = float64
itens_pedido.preco_unitario = float64
clientes.data_cadastro = string
 */

-- transfere a data do primeiro pedido para a data de cadastro do cliente e caso a data seja inválida
-- o safe.parse_date retorna nulo e o update corrige para a data do primeiro pedido

 update clientes c
 set c.data_cadastro = format_date('%m/%d/%Y', CAST(pp.primeiro_pedido as DATE))
 from (select id_cliente, min(data_pedido) as primeiro_pedido from pedidos group by id_cliente) pp
 where c.id_cliente = pp.id_cliente
 and ( SAFE.PARSE_DATE('%m/%d/%Y', c.data_cadastro) is null 
 or SAFE.PARSE_DATE('%m/%d/%Y', c.data_cadastro) > CAST(pp.primeiro_pedido as date));

 -- ajustar tipagem da coluna depois da padronização

 create or replace table clientes as 
 select * replace( parse_date ('%m/%d/%Y', data_cadastro) as data_cadastro)
from clientes;

-- verifiquei novamente o tipo de dados e a coluna foi ajustada

-- ajuste de valores

create or replace table produtos as select * replace ( cast (preco as numeric)  as preco)
from produtos;

create or replace table itens_pedido as select * replace ( cast (preco_unitario as numeric) as preco_unitario)
from itens_pedido;

create or replace table pedidos as select * replace ( cast (valor_total as numeric) as valor_total)
from pedidos;

-- schema checado novamente e os tipos foram alterados, resta apenas alterar os IDs de pedidos e inserir clientes
create or replace table pedidos as select *, row_number () over(order by data_pedido, id_pedido)
as id_pedido_novo from pedidos;

create or replace table itens_pedido as select i.*, p.id_pedido_novo 
from itens_pedido i join pedidos p on i.id_pedido = p.id_pedido;

--verifiquei os dados novamente e o id_pedido_novo foi adicionado as duas tabelas

-- adição de clientes novos na tabela de clientes

insert into clientes (id_cliente, nome, estado, data_cadastro) 
select distinct p.id_cliente, 'Desconhecido', 'DC', current_date() as data_cadastro
from pedidos p where p.id_cliente is not null and p.id_cliente not in (select id_cliente from clientes);

-- dei um select max na tabela de pedidos e o id mais alto era 200, 
-- depois verifiquei na tabela de clientes e o valor bateu 