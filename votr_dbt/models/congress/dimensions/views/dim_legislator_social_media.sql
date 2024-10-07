{{ config(
    materialized='view'
) }}

with current_members as (
    select *
    from {{ ref('dim_legislator_social_media_historical') }}
    where is_current_member
)

select * from current_members
