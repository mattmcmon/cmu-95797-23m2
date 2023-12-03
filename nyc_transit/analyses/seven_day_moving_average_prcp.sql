-- Calculate the 7 day moving average precipitation for every day in the weather data
-- Used https://mode.com/sql-tutorial/sql-window-functions#lag-and-lead for syntax on removing nulls in 7 day moving average
select *
from(    
        select    
            date,
            prcp,
            (
                (lag(prcp, 3) over (order by date) +
                lag(prcp, 2) over (order by date) +
                lag(prcp, 1) over (order by date) +
                prcp +
                lead(prcp, 1) over (order by date) +
                lead(prcp, 2) over (order by date) +
                lead(prcp, 3) over (order by date))/7
            ) as seven_day_avg_prcp
        from {{ ref('stg__central_park_weather') }}
        order by date
    ) sub
where sub.seven_day_avg_prcp is not null


