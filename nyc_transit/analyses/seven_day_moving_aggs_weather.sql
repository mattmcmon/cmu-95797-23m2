-- Calculate the 7 day moving min, max, avg, sum for precipitation and snow for every day in the weather data, defining the window only once.
select 
    date,
    min(prcp) over seven_day_moving_avg as min_prcp,
    max(prcp) over seven_day_moving_avg as max_prcp,
    avg(prcp) over seven_day_moving_avg as avg_prcp,
    sum(prcp) over seven_day_moving_avg as sum_prcp,
    min(snow) over seven_day_moving_avg as min_snow,
    max(snow) over seven_day_moving_avg as max_snow,
    avg(snow) over seven_day_moving_avg as avg_snow,
    sum(snow) over seven_day_moving_avg as sum_snow
from {{ ref('stg__central_park_weather')}}
window seven_day_moving_avg as (
    order by date asc
    range between interval 3 days preceding and interval 3 days following)