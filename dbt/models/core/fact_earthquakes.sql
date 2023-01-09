{{ config(
    materialized='incremental',
    partition_by={
      "field": "properties_time_datetime",
      "data_type": "timestamp",
      "granularity": "day"
    }
)}}

select *
from {{ ref('stg_earthquakes') }}