-- Fetches data from raw fhvhv_tripdata
with source as (

    select * from {{ source('main', 'fhvhv_tripdata') }}

),

-- Selects columns from fhvhv_tripdata, does data type conversion and cleaning where needed
renamed as (

    select
        hvfhs_license_num,
        trim(upper(dispatching_base_num)) as dispatching_base_num,
        trim(upper(originating_base_num)) as originating_base_num,
        request_datetime,
        on_scene_datetime,
        pickup_datetime,
        dropoff_datetime,
        pulocationid,
        dolocationid,
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
        {{flag_to_bool("shared_request_flag")}} as shared_request_flag,
        {{flag_to_bool("shared_match_flag")}} as shared_match_flag,
        {{flag_to_bool("access_a_ride_flag")}} as access_a_ride_flag,
        {{flag_to_bool("wav_request_flag")}} as wav_request_flag,
        {{flag_to_bool("wav_match_flag",)}} as wav_match_flag,
        filename

    from source
      WHERE request_datetime < TIMESTAMP '2022-12-31' -- drop rows in the future
        AND pickup_datetime < TIMESTAMP '2022-12-31' -- drop rows in the future
        AND dropoff_datetime < TIMESTAMP '2022-12-31' -- drop rows in the future

)

-- Selects all columns from the above renaming and data type conversions 
select * from renamed
