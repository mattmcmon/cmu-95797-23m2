-- Fetches data from bike_data
with source as (

    select * from {{ source('main', 'bike_data') }}

),

-- Used https://www.w3schools.com/sql/sql_datatypes.asp for refresher on SQL data types
-- Selects columns from bike_data and does data type conversion, if needed
renamed as (

    select
        tripduration,
        starttime,
        stoptime,
        "start station id",
        "start station name",
        "start station latitude",
        "start station longitude",
        "end station id",
        "end station name",
        "end station latitude",
        "end station longitude",
        bikeid,
        usertype,
        "birth year",
        gender,
        ride_id,
        rideable_type,
        started_at,
        ended_at,
        start_station_name,
        start_station_id,
        end_station_name,
        end_station_id,
        start_lat,
        start_lng,
        end_lat,
        end_lng,
        member_casual,
        filename

    from source

)

-- Select all columns from the above data type conversions and merges some columns that appear to be the same information
-- Used https://stackoverflow.com/questions/47094631/combine-two-columns-data-into-one-column-leaving-null-values for help with merging columns
-- Didn't include columns with large percentage of NULL values (determined by SUMMARIZE function). The bikeid, usertype, rideable_type, ride_id, gender and birth year had ~50% null values.
select
	coalesce(starttime, started_at)::timestamp as started_at_ts,
	coalesce(stoptime, ended_at)::timestamp as ended_at_ts,
    -- Calculates the difference (in seconds) between the start and stop timestamps if there is a null value in trip duration
    -- Used https://www.w3schools.com/sql/func_sqlserver_datediff.asp to help with syntax
	coalesce(tripduration::int,datediff('second', started_at_ts, ended_at_ts)) tripduration,
	coalesce("start station id", start_station_id) as start_station_id,  
	coalesce("start station name", start_station_name) as start_station_name,
	coalesce("start station latitude", start_lat)::double as start_lat,
	coalesce("start station longitude", start_lng)::double as start_lng, 
	coalesce("end station id", end_station_id) as end_station_id,  
	coalesce("end station name", end_station_name) as end_station_name,
	coalesce("end station latitude", end_lat)::double as end_lat,
	coalesce("end station longitude", end_lng)::double as end_lng,
	filename
from renamed