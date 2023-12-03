-- Finds taxi trips which don't have a pickup location ID in the locations table
select
    taxi.*
from {{ ref('mart__fact_all_taxi_trips')}} taxi
left join {{ ref('mart__dim_locations')}} loc on loc.locationid = taxi.pulocationid
where loc.locationid is null
limit 100