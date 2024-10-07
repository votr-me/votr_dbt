{{ config(
    materialized='table'
) }}

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
from {{ ref('stg_bill_info') }}
