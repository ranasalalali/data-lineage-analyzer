{{ config(materialized='view') }}

select *
from {{ source('silver', 'orders') }}
