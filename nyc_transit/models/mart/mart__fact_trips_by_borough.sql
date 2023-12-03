select
    borough,
    count(*) as total_trips
from {{ ref('mart__dim_locations') }} loc 
join {{ ref('mart__fact_all_taxi_trips')}} taxi on taxi.pulocationid = loc.locationid
group by borough
order by borough

