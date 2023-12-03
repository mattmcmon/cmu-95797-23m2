-- Calculate the number of trips and average duration by borough and zone
select 
    borough,
    zone,
    count(*) as trips,
    -- Displays average minutes
    avg(duration_min) as avg_dur_min,
    -- Displays average seconds
    avg(duration_sec) as avg_dur_sec
from {{ ref('mart__fact_all_taxi_trips') }} taxi
join {{ ref('mart__dim_locations') }} loc on taxi.pulocationid = loc.locationid
group by borough, zone