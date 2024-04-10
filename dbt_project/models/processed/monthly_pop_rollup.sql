{{ config(materialized='incremental') }}

select
    *
from {{ ref("weekly_pop_rollup") }}
{% if is_incremental() %}
where population_date >= '{{ var('min_date') }}' and population_date <= '{{ var('max_date') }}'
{% endif %}