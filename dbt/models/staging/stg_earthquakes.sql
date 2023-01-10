{{ config(materialized='view') }}

with earthquakes as 
(
    select *,
        row_number() over(partition by id, properties_updated_datetime) as rn
    from {{ source('raw','raw_earthquakes') }}
    where id is not null 
)
select 
    cast(type as STRING) as feature_type,
    cast(id as STRING) as id,
    cast(properties_mag as FLOAT64) as magnitude,
    cast(properties_place as STRING) as place,
    cast(properties_time as INTEGER) as origin_timestamp,
    cast(properties_updated as INTEGER) as updated_timestamp,
    cast(properties_tz as INTEGER) as timezone_offset,
    cast(properties_url as STRING) as url_link,
    cast(properties_detail as STRING) as properties_detail,
    cast(properties_felt as INTEGER) as felt_submissions,
    cast(properties_cdi as INTEGER) as cdi_inensity,
    cast(properties_mmi as FLOAT64) as maximum_instrumental_intensity,
    cast(properties_alert as STRING) as alert,
    cast(properties_status as STRING) as status,
    cast(properties_tsunami as INTEGER) as tsunami,
    cast(properties_sig as INTEGER) as significant_number,
    cast(properties_net as STRING) as network_id,
    cast(properties_code as STRING) as properties_code,
    cast(properties_ids as STRING) as properties_ids,
    cast(properties_sources as STRING) as properties_sources,
    cast(properties_types as STRING) as properties_types,
    cast(properties_nst as FLOAT64) as number_stations_total,
    cast(properties_dmin as FLOAT64) as minimum_horizontal_distance_to_station,
    cast(properties_rms as FLOAT64) as root_mean_square_travel_time,
    cast(properties_gap as FLOAT64) as  gap_between_stations,
    cast(properties_magType as STRING) as magType,
    cast(properties_type as STRING) as properties_type,
    cast(properties_title as STRING) as properties_title,
    cast(geometry_type as STRING) as geometry_type,
    geometry_coordinates as geometry_coordinates,
    cast(generated as INTEGER) as generated,
    cast(url as STRING) as url_string,
    cast(title as STRING) as title,
    cast(status as INTEGER) as review_status,
    cast(api as STRING) as api,
    cast(count as INTEGER) as count,
    cast(geometry_coordinates_longitude as FLOAT64) as geometry_coordinates_longitude,
    cast(geometry_coordinates_latitude as FLOAT64) as geometry_coordinates_latitude,
    cast(geometry_coordinates_depth as FLOAT64) as earthquake_depth,
    cast(properties_time_datetime as TIMESTAMP) as properties_time_datetime,
    cast(properties_updated_datetime as TIMESTAMP) as properties_updated_datetime,
    cast(metadata_generated_datetime as TIMESTAMP) as metadata_generated_datetime,
    cast(dataframe_timestamp_now as TIMESTAMP) as dataframe_timestamp_now,
    cast(rn as INTEGER) as rn
from earthquakes 
where rn = 1