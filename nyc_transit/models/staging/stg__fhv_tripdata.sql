-- Fetches data from raw fhv_tripdata
with source as (

    select * from {{ source('main', 'fhv_tripdata') }}

),

-- Selects columns from fhv_bases and does data type conversion, if needed
-- Left out SR_Flag because it was filled with null values
-- No changes made to data types, already in proper types 
renamed as (

    select
        trim(upper(dispatching_base_num)) as  dispatching_base_num, --some ids are lowercase
        pickup_datetime,
        dropoff_datetime,
        pulocationid,
        dolocationid,
        -- Didn't include sr_flag because it's always null
        trim(upper(affiliated_base_number)) as affiliated_base_number,
        filename

    from source
      WHERE dropoff_datetime < TIMESTAMP '2022-12-31' -- drop rows in the future
)

-- Selects all columns from the above renaming and data type conversions 
select * from renamed