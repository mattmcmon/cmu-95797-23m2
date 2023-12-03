--Finds the average time between taxi pick ups per zone
with taxi_trips as (select 
    zone,
    -- Calculate time difference using lead, finds next trip per zone for each record
    datediff('second',pickup_datetime,lead(pickup_datetime) over (partition by zone order by pickup_datetime)) as time_diff
from {{ ref('mart__fact_all_taxi_trips') }} taxi
join {{ ref('mart__dim_locations') }} loc on loc.locationid = taxi.pulocationid
)

select 
    taxi_trips.zone as zone,
    -- Calcualtes average time between pickups per zone by re-using CTE above
    avg(taxi_trips.time_diff) as average_time_between_pickups
from taxi_trips
group by zone
