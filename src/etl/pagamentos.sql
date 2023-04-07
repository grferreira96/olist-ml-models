-- Databricks notebook source
-- Tabela com sumarização dos dados dos últimos 6 meses para cada vendedor

WITH tb_pedidos AS ( 
  -- Remove pedidos duplicados
  SELECT 
      DISTINCT 
      t1.idPedido,
      t2.idVendedor
  FROM silver.olist.pedido AS t1
  LEFT JOIN silver.olist.item_pedido as t2
    ON t1.idPedido = t2.idPedido

  WHERE t1.dtPedido < '2018-01-01'
  AND t1.dtPedido >= add_months('2018-01-01', -6)
  AND idVendedor IS NOT NULL
),

tb_join AS (
  -- Informações dos vendedores, seus pedidos e como foram realizados os pagamentos
  SELECT 
    t1.idVendedor,
    t2.*         
  FROM tb_pedidos AS t1
  LEFT JOIN silver.olist.pagamento_pedido AS t2
  ON t1.idPedido = t2.idPedido
),

tb_group AS (
  -- Para cada vendedor, temos os tipos de pagamentos, o número de pedidos por
  -- meio de pagamento e o total recebido por cada meio de pagamento
  SELECT 
    idVendedor,
    descTipoPagamento,
    count(distinct idPedido) as qtdePedidoMeioPagamento,
    sum(vlPagamento) as vlPedidoMeioPagamento
  FROM tb_join
  GROUP BY idVendedor, descTipoPagamento
  ORDER BY idVendedor, descTipoPagamento
),

tb_summary AS (
  -- Sumarização do tipo de pagamento por cliente
  SELECT 
    idVendedor,

    sum(case when descTipoPagamento='boleto' then qtdePedidoMeioPagamento else 0 end) as qtde_boleto_pedido,
    sum(case when descTipoPagamento='credit_card' then qtdePedidoMeioPagamento else 0 end) as qtde_credit_card_pedido,
    sum(case when descTipoPagamento='voucher' then qtdePedidoMeioPagamento else 0 end) as qtde_voucher_pedido,
    sum(case when descTipoPagamento='debit_card' then qtdePedidoMeioPagamento else 0 end) as qtde_debit_card_pedido,

    sum(case when descTipoPagamento='boleto' then vlPedidoMeioPagamento else 0 end) as valor_boleto_pedido,
    sum(case when descTipoPagamento='credit_card' then vlPedidoMeioPagamento else 0 end) as valor_credit_card_pedido,
    sum(case when descTipoPagamento='voucher' then vlPedidoMeioPagamento else 0 end) as valor_voucher_pedido,
    sum(case when descTipoPagamento='debit_card' then vlPedidoMeioPagamento else 0 end) as valor_debit_card_pedido,

    sum(case when descTipoPagamento='boleto' then qtdePedidoMeioPagamento else 0 end) / sum(qtdePedidoMeioPagamento) as pct_qtd_boleto_pedido,
    sum(case when descTipoPagamento='credit_card' then qtdePedidoMeioPagamento else 0 end) / sum(qtdePedidoMeioPagamento) as pct_qtd_credit_card_pedido,
    sum(case when descTipoPagamento='voucher' then qtdePedidoMeioPagamento else 0 end) / sum(qtdePedidoMeioPagamento) as pct_qtd_voucher_pedido,
    sum(case when descTipoPagamento='debit_card' then qtdePedidoMeioPagamento else 0 end) / sum(qtdePedidoMeioPagamento) as pct_qtd_debit_card_pedido,

    sum(case when descTipoPagamento='boleto' then vlPedidoMeioPagamento else 0 end) / sum(vlPedidoMeioPagamento) as pct_valor_boleto_pedido,
    sum(case when descTipoPagamento='credit_card' then vlPedidoMeioPagamento else 0 end) / sum(vlPedidoMeioPagamento) as pct_valor_credit_card_pedido,
    sum(case when descTipoPagamento='voucher' then vlPedidoMeioPagamento else 0 end) / sum(vlPedidoMeioPagamento) as pct_valor_voucher_pedido,
    sum(case when descTipoPagamento='debit_card' then vlPedidoMeioPagamento else 0 end) / sum(vlPedidoMeioPagamento) as pct_valor_debit_card_pedido
  FROM tb_group
  GROUP BY idVendedor
),

tb_cartao as (
  -- Valores de média, mediana, mínimo e máximo quando o tipo de pagamento é 
  -- cartão de crédito para cada vendedor
  SELECT 
    idVendedor,
    avg(nrParcelas) AS avgQtdeParcelas,
    percentile(nrParcelas, 0.5) AS medianQtdeParcelas,
    max(nrParcelas) AS maxQtdeParcelas,
    min(nrParcelas) AS minQtdeParcelas
  FROM tb_join
  WHERE descTipoPagamento = 'credit_card'
  GROUP BY idVendedor
)

SELECT 
       '2018-01-01' AS dtReference,
       t1.*,
       t2.avgQtdeParcelas,
       t2.medianQtdeParcelas,
       t2.maxQtdeParcelas,
       t2.minQtdeParcelas
FROM tb_summary as t1
LEFT JOIN tb_cartao as t2
ON t1.idVendedor = t2.idVendedor

-- COMMAND ----------


