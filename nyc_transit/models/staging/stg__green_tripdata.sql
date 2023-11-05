
-- Fetches data from raw green_tripdata
with source as (

    select * from {{ source('main', 'green_tripdata') }}

),

-- Selects columns from green_tripdata and renames them, as well as data type conversion
--Excluded ehail_fee from data since it was 100% null
renamed as (

    select
        VendorID as vendor_id,
        lpep_pickup_datetime,
        lpep_dropoff_datetime,
        -- Converts varchar to boolean
        case
            when store_and_fwd_flag = 'Y' then true
            when store_and_fwd_flag = 'N' then false
            else null
        end as store_and_fwd_flag,
        case
            when RatecodeID is null then 0
            else RatecodeID
        end as rate_code_id,
        PULocationID as pu_loc_id,
        DOLocationID as do_loc_id,
        passenger_count,
        trip_distance,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        total_amount,
        payment_type,
        trip_type,
        congestion_surcharge,
        filename
    from source

)

-- Selects all columns from the above renaming and data type conversions 
select * from renamed
where (lpep_pickup_datetime <= '2022-12-31 00:00:00')
and (lpep_dropoff_datetime <= '2022-12-31 00:00:00')
and (rate_code_id < 7 or rate_code_id is null)
and (trip_distance > 0)
and (passenger_count > 0 or passenger_count is null)
and (fare_amount > 0)
and (extra >= 0)
and (mta_tax >= 0)
and (tip_amount >= 0)
and (improvement_surcharge >= 0)
and (total_amount > 0)
and (congestion_surcharge > 0 or congestion_surcharge is null)