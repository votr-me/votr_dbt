
{{ config(
    materialized='view'
) }}

with current_congress_bills as (
    select
        bill_id,
        bill_type,
        bill_number,
        congress,
        origin_chamber,
        bill_title,
        latest_action_date,
        latest_action_text,
        bill_introduced_date,
        policy_area_name
    from {{ ref('dim_bill_info_historical') }}
    where
    congress = (
        select max(congress)
        from {{ ref('dim_bill_info_historical') }}
    )
)

select * from current_congress_bills