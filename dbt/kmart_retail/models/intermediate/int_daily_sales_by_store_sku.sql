{{ config(materialized='table') }}
{% set trading_date_sql = "cast('" ~ var('trading_date') ~ "' as date)" %}

with history as (
  select
    sale_date,
    store_id,
    product_sku,
    units_sold,
    sales_ex_gst
  from {{ source('silver', 'sales_history') }}
  where sale_date >= {{ retail_dateadd_days(trading_date_sql, -28) }}
    and sale_date < cast('{{ var("trading_date") }}' as date)
),
current_day as (
  select
    trading_date as sale_date,
    store_id,
    product_sku,
    cast(sum(qty) as integer) as units_sold,
    cast(sum(line_sales_ex_gst) as decimal(14,2)) as sales_ex_gst
  from {{ ref('stg_pos_transactions') }}
  group by 1, 2, 3
)
select * from history
union all
select * from current_day
