-- Fetches data from raw fhv_tripdata
with source as (

    select * from {{ source('main', 'fhv_tripdata') }}

),

-- Selects columns from fhv_bases and does data type conversion, if needed
-- Left out SR_Flag because it was filled with null values
-- No changes made to data types, already in proper types 
renamed as (

    select
        dispatching_base_num,
        pickup_datetime,
        dropOff_datetime as dropoff_datetime,
        PUlocationID as pu_loc_id,
        DOlocationID as do_location_id,
        Affiliated_base_number,
        filename
    from source

)

-- Selects all columns from the above renaming and data type conversions 
select * from renamed
where (dropoff_datetime < '2022-12-31 00:00:00')