{{ config(materialized='view') }}

with earthquakes as 
(
    select *,
        row_number() over(partition by id, properties_updated_datetime) as rn
    from {{ source('raw','raw_earthquakes') }}
    where id is not null 
)
select *
from earthquakes 
where rn = 1