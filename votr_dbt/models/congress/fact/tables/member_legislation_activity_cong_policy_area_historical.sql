{{ config(
    materialized='table'
) }}

with all_sponsors as (
    select distinct
        bioguide_id,
        bill_id,
        'sponsor' as sponsorship_type
    from {{ ref('dim_bill_sponsors_historical')}}
    union all
    select distinct
        bioguide_id,
        bill_id,
        'cosponsor' as sponsorship_type
    from {{ ref('dim_bill_cosponsors_historical') }}
)

select
    all_sponsors.bioguide_id,
    bill_info.congress,
    coalesce(bill_info.policy_area_name, 'None Reported') as policy_area_name,
    count(distinct CASE WHEN all_sponsors.sponsorship_type = 'sponsor' THEN all_sponsors.bill_id ELSE NULL END) as num_bills_sponsored,
    count(distinct CASE WHEN all_sponsors.sponsorship_type = 'cosponsor' THEN all_sponsors.bill_id ELSE NULL END) as num_bills_cosponsored
from all_sponsors
left join {{ ref('dim_bill_info_historical') }} bill_info
    on all_sponsors.bill_id = bill_info.bill_id
group by all_sponsors.bioguide_id, bill_info.congress, coalesce(bill_info.policy_area_name, 'None Reported')
