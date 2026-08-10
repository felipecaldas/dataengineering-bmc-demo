{{ config(materialized='view') }}

select
  store_id,
  trading_date,
  transaction_count,
  total_ex_gst,
  eod_ts_local,
  eod_ts_utc
from {{ source('silver', 'store_eod') }}
where trading_date = '{{ var("trading_date") }}'::date

