{{ config(materialized='table') }}

select
  concat(stock.store_id, ':', stock.product_sku) as store_sku_key,
  stock.store_id,
  stock.product_sku,
  stock.on_hand_units,
  stock.on_order_units,
  product.lead_time_days,
  product.review_period_days,
  product.safety_stock_units,
  product.is_active_line,
  stock.snapshot_date
from {{ ref('int_stock_on_hand') }} stock
join {{ ref('dim_product') }} product using (product_sku)

