-- Databricks notebook source
WITH tb_join AS (
  -- UF dos pedidos dos cliente: UF das vendas
  SELECT 
    DISTINCT 
    t1.idPedido,
    t1.idCliente,
    t2.idVendedor,
    t3.descUF
  FROM silver.olist.pedido AS t1

  LEFT JOIN silver.olist.item_pedido AS t2
  ON t1.idPedido = t2.idPedido

  LEFT JOIN silver.olist.cliente AS t3
  ON t1.idCliente = t3.idCliente

  WHERE t1.dtPedido < '2018-01-01'
    AND t1.dtPedido >= add_months('2018-01-01', -6)
    AND t2.idVendedor IS NOT NULL
),

tb_group(

  SELECT
    idVendedor,
    
    count(DISTINCT descUF) as qtdUFsPedidos,

    count(CASE WHEN descUF='AC' THEN idPedido END) / count(DISTINCT idPedido) AS pctPedidoAC,
    count(CASE WHEN descUF='AL' THEN idPedido END) / count(DISTINCT idPedido) AS pctPedidoAL,
    count(CASE WHEN descUF='AM' THEN idPedido END) / count(DISTINCT idPedido) AS pctPedidoAM,
    count(CASE WHEN descUF='AP' THEN idPedido END) / count(DISTINCT idPedido) AS pctPedidoAP,
    count(CASE WHEN descUF='BA' THEN idPedido END) / count(DISTINCT idPedido) AS pctPedidoBA,
    count(CASE WHEN descUF='CE' THEN idPedido END) / count(DISTINCT idPedido) AS pctPedidoCE,
    count(CASE WHEN descUF='DF' THEN idPedido END) / count(DISTINCT idPedido) AS pctPedidoDF,
    count(CASE WHEN descUF='ES' THEN idPedido END) / count(DISTINCT idPedido) AS pctPedidoES,
    count(CASE WHEN descUF='GO' THEN idPedido END) / count(DISTINCT idPedido) AS pctPedidoGO,
    count(CASE WHEN descUF='MA' THEN idPedido END) / count(DISTINCT idPedido) AS pctPedidoMA,
    count(CASE WHEN descUF='MG' THEN idPedido END) / count(DISTINCT idPedido) AS pctPedidoMG,
    count(CASE WHEN descUF='MS' THEN idPedido END) / count(DISTINCT idPedido) AS pctPedidoMS,
    count(CASE WHEN descUF='MT' THEN idPedido END) / count(DISTINCT idPedido) AS pctPedidoMT,
    count(CASE WHEN descUF='PA' THEN idPedido END) / count(DISTINCT idPedido) AS pctPedidoPA,
    count(CASE WHEN descUF='PB' THEN idPedido END) / count(DISTINCT idPedido) AS pctPedidoPB,
    count(CASE WHEN descUF='PE' THEN idPedido END) / count(DISTINCT idPedido) AS pctPedidoPE,
    count(CASE WHEN descUF='PI' THEN idPedido END) / count(DISTINCT idPedido) AS pctPedidoPI,
    count(CASE WHEN descUF='PR' THEN idPedido END) / count(DISTINCT idPedido) AS pctPedidoPR,
    count(CASE WHEN descUF='RJ' THEN idPedido END) / count(DISTINCT idPedido) AS pctPedidoRJ,
    count(CASE WHEN descUF='RN' THEN idPedido END) / count(DISTINCT idPedido) AS pctPedidoRN,
    count(CASE WHEN descUF='RO' THEN idPedido END) / count(DISTINCT idPedido) AS pctPedidoRO,
    count(CASE WHEN descUF='RR' THEN idPedido END) / count(DISTINCT idPedido) AS pctPedidoRR,
    count(CASE WHEN descUF='RS' THEN idPedido END) / count(DISTINCT idPedido) AS pctPedidoRS,
    count(CASE WHEN descUF='SC' THEN idPedido END) / count(DISTINCT idPedido) AS pctPedidoSC,
    count(CASE WHEN descUF='SE' THEN idPedido END) / count(DISTINCT idPedido) AS pctPedidoSE,
    count(CASE WHEN descUF='SP' THEN idPedido END) / count(DISTINCT idPedido) AS pctPedidoSP,
    count(CASE WHEN descUF='TO' THEN idPedido END) / count(DISTINCT idPedido) AS pctPedidoTO
  FROM tb_join
  GROUP BY 1
)

SELECT 
  '2018-01-01' AS dtReference,
  *
FROM 
  tb_group

-- COMMAND ----------


