{{ config(materialized='incremental') }}

select
    *,
    current_date as population_date
from {{ ref("cleaned_population") }}
{% if is_incremental() %}
where population_date >= '{{ var('min_date') }}' and population_date <= '{{ var('max_date') }}'
{% endif %}
