-- Displays the # of total trips, # trips starting and ending in different borough, and % w/ different start and end
-- Used https://learnsql.com/blog/what-is-common-table-expression/ for refresher on Common Table Expressions (implemented below)

-- Defines temp results with all taxi trip counts by weekday
with taxi_trips as
(select 
    weekday(pickup_datetime) as weekday, 
    count(*) as total_trips
    from {{ ref('mart__fact_all_taxi_trips') }} as taxi
    group by all
),

-- Defines second set of temp results that stores the total number of trips by weekday
-- that accounts for different start and end borough
diff_boroughs as
(select 
    weekday(pickup_datetime) as weekday, 
    count(*) as total_trips
from {{ ref('mart__fact_all_taxi_trips') }} as taxi
join {{ ref('mart__dim_locations') }} as pickup
    on taxi.pulocationid = pickup.locationid
join {{ ref('mart__dim_locations') }} as dropoff
    on taxi.dolocationid = dropoff.locationid
where pickup.borough != dropoff.borough
group by all
)

-- Joins the two temp results from above
-- Displays the total trips, trips that start and end at different boroughs, and calculates the percentage 
-- of trips that start and end at different boroughs
select taxi_trips.weekday,
       taxi_trips.total_trips as all_trips,
       diff_boroughs.total_trips as diff_borough_trips,
       diff_boroughs.total_trips / taxi_trips.total_trips as percent_diff_borough
from taxi_trips
join diff_boroughs 
    on (taxi_trips.weekday = diff_boroughs.weekday);