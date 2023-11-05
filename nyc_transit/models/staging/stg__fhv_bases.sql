-- Fetches data from raw fhv_bases
with source as (

    select * from {{ source('main', 'fhv_bases') }}

),

-- Selects columns from fhv_bases and does data type conversion, if needed
-- Didn't include dba column because it was 80% null values
renamed as (

    select
        base_number,
        base_name,
        dba_category,
        filename

    from source

)

-- Selects all columns from the above renaming and data type conversions

select * from renamed