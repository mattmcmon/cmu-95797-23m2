-- Fetches data from raw fhv_bases
with source as (

    select * from {{ source('main', 'fhv_bases') }}

),

-- Selects columns from fhv_bases, updates names and data types where needed
renamed as (

    select
        -- clean up the base_num to be properly linked as foreign keys
        trim(upper(base_number)) as base_number,
        base_name,
        dba,
        dba_category,
        filename

    from source

)

-- Selects all columns from the above renaming and data type conversions
select * from renamed