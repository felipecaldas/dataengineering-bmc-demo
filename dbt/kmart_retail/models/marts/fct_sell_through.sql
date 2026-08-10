{{ config(materialized='table') }}

select
  store_id,
  product_sku,
  sum(units_sold) as units_sold_28d,
  sum(sales_ex_gst)::numeric(16,2) as sales_ex_gst_28d,
  avg(units_sold)::numeric(12,3) as avg_daily_units
from {{ ref('int_daily_sales_by_store_sku') }}
group by 1, 2

