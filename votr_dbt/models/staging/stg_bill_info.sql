{{ config(materialized='incremental') }}
with bill_info_cte as (
    select
        bill_id,
        number::int as bill_number,
        congress::int as congress,
        title as bill_title,
        "latestAction.actionDate"::date as latest_action_date,
        "latestAction.text" as latest_action_text,
        lower(type) as bill_type,
        lower("originChamber") as origin_chamber
    from {{ source('raw', 'bill_info') }}
),

bill_details_cte as (
    select
        bill_id,
        "introducedDate"::date as bill_introduced_date,
        "policyArea.name" as policy_area_name
    from {{ source('raw', 'bill_details') }}
)

select
    bi.bill_id,
    bi.bill_type,
    bi.bill_number,
    bi.congress,
    bi.origin_chamber,
    bi.bill_title,
    bi.latest_action_date,
    bi.latest_action_text,
    bd.bill_introduced_date,
    bd.policy_area_name
from bill_info_cte as bi
left join bill_details_cte as bd
    on bi.bill_id = bd.bill_id
order by
    bi.congress desc,
    bi.bill_type asc,
    bi.bill_number desc
