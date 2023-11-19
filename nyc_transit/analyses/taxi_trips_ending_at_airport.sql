-- Displays the total number of trips that end in 'Airports' or 'EWR'
select
    count(*) as total_trips,
from {{ ref('mart__fact_all_taxi_trips') }} as taxis
JOIN {{ ref('mart__dim_locations') }} as locations
ON taxis.dolocationid=locations.LocationID
-- Used https://www.w3schools.com/sql/sql_where.asp for reminder on WHERE with multiple values
WHERE locations.service_zone in ('Airports', 'EWR')