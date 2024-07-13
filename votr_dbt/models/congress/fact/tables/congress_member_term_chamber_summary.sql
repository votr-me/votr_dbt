
{{ config(
    materialized='table'
) }}

with term_summary as (
    select
        bioguide_id,
        chamber,
        is_current_member,
        count(*) as term_count,
        min(start_year) as first_year_in_chamber,
        max(end_year) as last_year_in_chamber,
        max(end_year) - min(start_year) +1 as total_years_served,
        count(distinct congress) as num_congresses_served
    from {{ ref('stg_congress_member_terms')}}
    group by bioguide_id, chamber, is_current_member
)

select * from term_summary