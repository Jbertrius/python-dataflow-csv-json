# python-dataflow-csv-json

####I. Quels sont les éléments à considérer pour faire évoluer votre code afin qu’il puisse gérer de grosses volumétries de données (fichiers de plusieurs To ou millions de fichiers par exemple) ? Pourriez-vous décrire les modifications qu’il faudrait apporter, s’il y en a, pour prendre en considération de telles volumétries ?

``
Les éléments à considerer pour faire evoluer le code pour de grosse volumétrie, serait le nombre total de workers (machines) qui execute le traitement, et la mémoire dont dispose ces machines pour assurer le traitement. 
``

* **Au niveau du code, on pourrait changer les parametres en entrées du traitement Dataflow pour choisir des workers ayant plus de capacité.**
* **Pour la liste total des médicaments (Side Input) qui peut très vite occupé beaucoup de mémoire, on peut assurer sa persistence, en l'enregistrant dans une base de données. De cette maniere au prochain traitement, on le chargerait dans le traitement courant plutot que de le recalculer.**


####II. SQL

```mysql-sqlWITH all_sales AS
( SELECT date, sum(prod_price * prod_qty) as ventes FROM `PROJECT.DATASET.transaction`  group by date  order by date asc )

SELECT  FORMAT_DATE("%d/%m/%Y", date) as date, ventes from all_sales where extract (year from date) = 2020
```
```mysql-sql
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
```