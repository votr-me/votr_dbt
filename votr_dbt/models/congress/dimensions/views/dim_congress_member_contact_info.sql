
{{ config(
    materialized='view'
) }}

with current_members as (
    select *
    from {{ ref('dim_congress_member_contact_info_historical') }}
    where is_current_member
)

select * from current_members