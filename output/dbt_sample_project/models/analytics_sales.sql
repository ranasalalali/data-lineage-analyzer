{{ config(materialized='view', docs={'node_color': 'red'}) }}

select *
from {{ ref('staging_sales') }}
