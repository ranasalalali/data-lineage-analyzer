{{ config(materialized='view') }}

select *
from {{ ref('staging_orders') }}
