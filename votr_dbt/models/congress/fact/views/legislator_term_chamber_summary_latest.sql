{{ config(
    materialized='view'
) }}


select *
from {{ ref('legislator_term_chamber_summary') }}
where is_current_member
