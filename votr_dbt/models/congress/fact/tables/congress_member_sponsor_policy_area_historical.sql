{{ config(
    materialized='table'
) }}

with bill_sponsors as (
    select *
    from {{ ref('dim_bill_sponsors_historical') }}
),
bill_info as (
    select *
    from {{ ref('dim_bill_info_historical') }}
)
select
    a.bioguide_id,
    b.policy_area_name,
    count(distinct b.bill_id) as num_bills_sponsored_bills
from
    bill_sponsors a
left join
    bill_info b
    on a.bill_id = b.bill_id
group by
    a.bioguide_id,
    b.policy_area_name