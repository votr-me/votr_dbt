{{ config(materialized='incremental', primary_key='id') }}
with bill_info_cte as (
    select
        bill_id,
        SAFE_CAST(number AS INT64) AS bill_number,
        SAFE_CAST(congress AS INT64) AS congress,
        title AS bill_title,
        CAST(NULLIF(`latestAction_actionDate`, 'nan') AS DATE) AS latest_action_date,
        `latestAction_text` AS latest_action_text,
        LOWER(type) AS bill_type,
        LOWER(`originChamber`) AS origin_chamber
    from {{ source('raw', 'bill_info') }}
),

bill_details_cte as (
    select
        bill_id,
        CAST(NULLIF(introducedDate, 'nan') AS TIMESTAMP) AS bill_introduced_date,
        `policyArea_name` AS policy_area_name
    from {{ source('raw', 'bill_details') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['bi.bill_id', 'bi.bill_type', 'bi.bill_number', 'bi.congress', 'bi.origin_chamber']) }} AS id,
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
