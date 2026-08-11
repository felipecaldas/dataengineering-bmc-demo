{{ config(materialized='view') }}

select
  transaction_id,
  trading_date,
  store_id,
  till_id,
  product_sku,
  qty,
  unit_price_ex_gst,
  qty * unit_price_ex_gst as line_sales_ex_gst,
  transaction_ts_local,
  transaction_ts_utc
from {{ source('silver', 'pos_transactions') }}
where trading_date = cast('{{ var("trading_date") }}' as date)
