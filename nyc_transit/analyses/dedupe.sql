-- Removes duplicate rows from "events" using Window functions with qualify and row_number
select  * 
from {{ ref('events') }}
qualify row_number() over (partition by event_id order by insert_timestamp desc) = 1