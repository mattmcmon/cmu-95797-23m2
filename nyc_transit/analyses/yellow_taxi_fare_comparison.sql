-- Compares individual fare to the zone, borough, and overall average fares
select
    -- Individual fare amount
    fare_amount,
    -- Calculates average fare for the zone
    avg(fare_amount) over (partition by zone) as avg_zone,
    -- Calculates average fare for the borough
    avg(fare_amount) over (partition by borough) as avg_borough,
    -- Calculates average fare overall
    avg(fare_amount) over () as avg_overall
from {{ ref('stg__yellow_tripdata') }} taxi
join {{ ref('mart__dim_locations') }} loc on taxi.pulocationid = loc.locationid
limit 100