{{ config(materialized='table') }}

select *
from {{ ref('stg_product_master') }}

