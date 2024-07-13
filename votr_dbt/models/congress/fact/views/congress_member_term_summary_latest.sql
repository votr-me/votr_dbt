
{{ config(
    materialized='view'
) }}


select * 
from {{ref('congress_member_term_summary')}}
where is_current_member