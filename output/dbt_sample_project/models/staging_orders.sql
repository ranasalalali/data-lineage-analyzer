{{ config(materialized='view') }}

select *
from (
	{{ source('silver', 'products') }} as t0
join
	{{ source('silver', 'customers') }} as t1
)
