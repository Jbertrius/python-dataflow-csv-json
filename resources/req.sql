--- Requete : Premiere partie du test

WITH all_sales AS
( SELECT date, sum(prod_price * prod_qty) as ventes FROM `PROJECT.DATASET.transaction`  group by date  order by date asc )

SELECT  FORMAT_DATE("%d/%m/%Y", date) as date, ventes from all_sales where extract (year from date) = 2020





---Requete : Seconde partie du test
WITH dataset as (
SELECT
  transact.date, transact.order_id, transact.client_id, transact.prod_price, transact.prod_qty,
  product.product_type, product.product_name
FROM
  `PROJECT.DATASET.transaction` AS transact
JOIN
  `PROJECT.DATASET.product_nomenclature` AS product
ON
  cast(transact.prod_id as INT64) = product.product_id

where product.product_type in ('MEUBLE', 'DECO')
and extract (year from date) = 2019
order by date
),

deco as (
SELECT client_id, sum(prod_price * prod_qty) as ventes_deco from ( select * from dataset where product_type = 'DECO')  group by client_id
),

meuble as (SELECT client_id, sum(prod_price * prod_qty) as ventes_meuble from ( select * from dataset where product_type = 'MEUBLE')  group by client_id )

select deco.client_id, deco.ventes_deco , meuble.ventes_meuble from deco join meuble on deco.client_id = meuble.client_id