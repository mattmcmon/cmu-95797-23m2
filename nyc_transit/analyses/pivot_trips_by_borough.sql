-- Sums trips by borough (done by pivoting mart__fact_trips_by_borough)
-- used documentation on https://github.com/dbt-labs/dbt-utils#pivot-source for dbt pivot macro
select  
        {{ dbt_utils.pivot(
        'borough',
        dbt_utils.get_column_values(ref('mart__fact_trips_by_borough'), 'borough'),
        agg='sum',
        then_value = 'total_trips',
        ) }}
from {{ ref('mart__fact_trips_by_borough') }}