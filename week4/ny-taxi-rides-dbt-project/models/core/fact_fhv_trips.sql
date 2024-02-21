{{
    config(
        materialized='table'
    )
}}

with fhv_tripdata as (
    select *
    from {{ ref('stg_fhv_tripdata') }}
    where pickup_location_id is not null
    and 
    dropoff_location_id is not null
), 
dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)

select *
from fhv_tripdata
-- this fails for no reason
inner join dim_zones as pickup_zone
on fhv_tripdata.pickup_location_id = pickup_zone.location_id
inner join dim_zones as dropoff_zone
on fhv_tripdata.dropoff_location_id = dropoff_zone.location_id