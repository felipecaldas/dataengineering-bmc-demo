{{ config(materialized='table') }}

select
  stock.store_id,
  stock.product_sku,
  stock.on_hand_units,
  stock.on_order_units,
  stock.snapshot_date
from {{ source('bronze', 'stock_on_hand') }} stock
where stock.snapshot_date = cast('{{ var("trading_date") }}' as date)
