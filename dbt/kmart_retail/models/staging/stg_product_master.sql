{{ config(materialized='view') }}

select
  product_sku,
  product_name,
  category,
  unit_cost,
  retail_price,
  lead_time_days,
  review_period_days,
  safety_stock_units,
  is_active_line
from {{ source('silver', 'product_master') }}

