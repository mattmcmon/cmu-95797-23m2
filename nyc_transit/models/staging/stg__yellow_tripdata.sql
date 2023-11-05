
-- Fetches data from raw yellow_tripdata
with source as (

    select * from {{ source('main', 'yellow_tripdata') }}

),

-- Selects columns from yellow_tripdata and renames them, as well as data type conversion
renamed as (

    select
        VendorID as vendor_id,
        tpep_pickup_datetime,
        tpep_dropoff_datetime,
        passenger_count,
        trip_distance,
        case
            when RatecodeID is null then 0
            else RatecodeID
        end as rate_code_id,
        -- Converts varchar to boolean
        case
            when store_and_fwd_flag = 'Y' then true
            when store_and_fwd_flag = 'N' then false
            else null
        end as store_and_fwd_flag,
        PULocationID as pu_loc_id,
        DOLocationID as do_loc_id,
        payment_type,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        total_amount,
        congestion_surcharge,
        airport_fee,
        filename
    from source

)

-- Selects all columns from the above renaming and data type conversions 
select * from renamed
where (passenger_count > 0 or passenger_count is null)
and (rate_code_id < 7 or rate_code_id is null)
and (trip_distance > 0)
and (fare_amount > 0)
and (extra > 0)
and (mta_tax > 0)
and (tip_amount >= 0)
and (tolls_amount >= 0)
and (improvement_surcharge >= 0)
and (total_amount > 0)
and (congestion_surcharge >= 0)
and (airport_fee >= 0 or airport_fee is null)