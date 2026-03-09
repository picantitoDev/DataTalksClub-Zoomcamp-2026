with non_null as (

    select *
    from {{ source('raw', 'fhv_tripdata') }}
    where dispatching_base_num is not null

),

filtered as (

    select 
        cast(dispatching_base_num as string) as dispatching_base_num,
        cast(pulocationid as int) as pickup_location_id,
        cast(dolocationid as int) as dropoff_location_id,
        cast(pickup_datetime as timestamp) as pickup_datetime,
        cast(dropoff_datetime as timestamp) as dropoff_datetime,
        cast(sr_flag as int64) as shared_ride_flag
    from non_null   

)

select *
from filtered