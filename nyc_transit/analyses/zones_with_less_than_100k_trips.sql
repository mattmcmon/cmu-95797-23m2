-- Finds all the Zones where there are less than 100,000 trips
select
    zone,
    -- Counts number of trips per zone
    count(*) as total_trips
from {{ ref('mart__dim_locations') }} loc 
join {{ ref('mart__fact_all_taxi_trips')}} taxi on taxi.pulocationid = loc.locationid
group by zone
-- Limits results to zones with less than 100k trips
having total_trips < 100000