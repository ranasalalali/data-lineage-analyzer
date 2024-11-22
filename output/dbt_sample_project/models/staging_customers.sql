{{ config(materialized='view', docs={'node_color': 'red'}) }}

select *
from {{ source('silver', 'customers') }}
