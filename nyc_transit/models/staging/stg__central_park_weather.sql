-- Fetches data from raw central_park_weather
with source as (

    select * from {{ source('main', 'central_park_weather') }}

),

-- Selects columns from central_park_weather and renames them, as well as data type conversion
renamed as (

    select
        station,
        name,
        date::date as date,
        awnd::double as awnd,
        prcp::double as prcp,
        snow::double as snow,
        snwd::double as snwd,
        tmax::int as tmax,
        tmin::int as tmin,
        filename

    from source

)

-- Selects all columns from the above renaming and data type conversions 
select 
    date,
    awnd,
    prcp,
    snow,
    snwd,
    tmax,
    tmin,
    filename
from renamed