
{{ config(
    materialized='view'
) }}

    select *
    from {{ ref('dim_congress_member_historical') }}
    where is_current_member