{{ config(materialized='table') }}

with target as (
  select
    position.store_id,
    position.product_sku,
    position.on_hand_units,
    position.on_order_units,
    velocity.avg_daily_units,
    ceil(
      velocity.avg_daily_units
      * (position.lead_time_days + position.review_period_days)
    )::integer + position.safety_stock_units as target_units
  from {{ ref('fct_stock_position') }} position
  join {{ ref('fct_sell_through') }} velocity
    on position.store_id = velocity.store_id
   and position.product_sku = velocity.product_sku
  where position.is_active_line
)
select
  store_id,
  product_sku,
  on_hand_units,
  on_order_units,
  avg_daily_units,
  target_units,
  greatest(0, target_units - on_hand_units - on_order_units)::integer
    as replenishment_units
from target
where greatest(0, target_units - on_hand_units - on_order_units) > 0

