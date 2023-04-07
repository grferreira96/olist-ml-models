-- Databricks notebook source
SHOW TABLES FROM silver.olist

-- COMMAND ----------

SELECT * FROM silver.olist.pedido limit 10

-- COMMAND ----------

SELECT * FROM silver.olist.pagamento_pedido limit 10

-- COMMAND ----------

SELECT * FROM silver.olist.item_pedido limit 10

-- COMMAND ----------

select * from silver.olist.item_pedido

-- COMMAND ----------

-- Como o Frete é cobrado em um Pedido?
WITH produtos_pPedidos AS (
  SELECT 
    idPedido,
    max(idPedidoItem) AS nProdutos 
  FROM 
    silver.olist.item_pedido
  GROUP BY 1
  ORDER BY 2 DESC
  LIMIT 1
)

SELECT 
  t2.idPedido,
  t2.idProduto,
  t2.vlFrete 
FROM 
  produtos_pPedidos t1 
LEFT JOIN silver.olist.item_pedido t2 
ON t1.idPedido = t2.idPedido
