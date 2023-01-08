{{ config(
    materialized='incremental',
    partition_by={
      "field": "metadata_generated_datetime",
      "data_type": "timestamp",
      "granularity": "day"
    }
)}}


select *
from {{ ref('stg_earthquakes') }} 
