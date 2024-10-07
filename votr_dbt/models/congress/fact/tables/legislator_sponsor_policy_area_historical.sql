{{ config(
    materialized='table'
) }}

with bill_sponsors as (
    select *
    from {{ ref('dim_bill_sponsorships_historical') }}
),

bill_info as (
    select *
    from {{ ref('dim_bill_info_historical') }}
)

select
    bill_sponsors.bioguide_id,
    bill_sponsors.sponsorship_type,
    coalesce (bill_info.policy_area_name, 'None') as policy_area_name,
    count(distinct bill_info.bill_id) as num_bills_sponsored
from
    bill_sponsors
left join
    bill_info
    on bill_sponsors.bill_id = bill_info.bill_id
group by
    bill_sponsors.bioguide_id,
    bill_info.policy_area_name,
    bill_sponsors.sponsorship_type
