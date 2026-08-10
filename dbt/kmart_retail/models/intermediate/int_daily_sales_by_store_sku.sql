{{ config(materialized='table') }}

with history as (
  select
    sale_date,
    store_id,
    product_sku,
    units_sold,
    sales_ex_gst
  from {{ source('silver', 'sales_history') }}
  where sale_date >= '{{ var("trading_date") }}'::date - interval '28 days'
    and sale_date < '{{ var("trading_date") }}'::date
),
current_day as (
  select
    trading_date as sale_date,
    store_id,
    product_sku,
    sum(qty)::integer as units_sold,
    sum(line_sales_ex_gst)::numeric(14,2) as sales_ex_gst
  from {{ ref('stg_pos_transactions') }}
  group by 1, 2, 3
)
select * from history
union all
select * from current_day

