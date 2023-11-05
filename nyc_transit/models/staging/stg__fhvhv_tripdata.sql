-- Fetches data from raw fhvhv_tripdata
with source as (

    select * from {{ source('main', 'fhvhv_tripdata') }}

),

-- Selects columns from fhvhv_tripdata and does data type conversion, if needed
-- Left out SR_Flag because it was filled with null values
-- No changes made to column names, naming format is already good
-- No changes made to data types, already in proper types 
renamed as (

    select
        hvfhs_license_num,
        dispatching_base_num,
        originating_base_num,
        request_datetime,
        on_scene_datetime,
        pickup_datetime,
        dropoff_datetime,
        PULocationID as pu_loc_id,
        DOLocationID as do_loc_id,
        trip_miles,
        trip_time,
        base_passenger_fare,
        tolls,
        bcf,
        sales_tax,
        congestion_surcharge,
        airport_fee,
        tips,
        driver_pay,
        -- Converts flag columns from strings to boolean
        -- Used https://www.w3schools.com/sql/sql_case.asp for documentation on case when statements
        case
            when shared_request_flag = 'Y' then true
            when shared_request_flag = 'N' then false
            else null
        end as shared_request_flag,
        case
            when shared_match_flag = 'Y' then true
            when shared_match_flag = 'N' then false
            else null
        end as shared_match_flag,
        case
            when access_a_ride_flag = ' ' then true
            when access_a_ride_flag = 'N' then false
            else null
        end as access_a_ride_flag,
        case
            when wav_request_flag = 'Y' then true
            when wav_request_flag = 'N' then false
            else null
        end as wav_request_flag,
        case
            when wav_match_flag = 'Y' then true
            when wav_match_flag = 'N' then false
            else null
        end as wav_match_flag,
        filename
    from source

)

-- Selects all columns from the above renaming and data type conversions 
select * from renamed
where (base_passenger_fare > 0)
and (driver_pay > 0)