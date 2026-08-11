{{ config(materialized='view') }}

select
  asn_id,
  trading_date,
  product_sku,
  expected_units,
  expected_arrival_date,
  supplier_id
from {{ source('silver', 'asn_inbound') }}
where trading_date = cast('{{ var("trading_date") }}' as date)
