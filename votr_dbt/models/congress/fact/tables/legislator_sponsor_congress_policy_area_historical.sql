{{ config(
    materialized='table',
    primary_key = 'id'
) }}

with bill_sponsors as (
    select *
    from {{ ref('dim_bill_sponsorships_historical') }}
),

bill_info as (
    select *
    from {{ ref('dim_bill_info_historical') }}
),

total_bills as (
    select
        bill_sponsors.bioguide_id,
        bill_info.congress,
        bill_sponsors.sponsorship_type,
        count(distinct bill_info.bill_id) as num_total_bills
    from
        bill_sponsors
    left join
        bill_info
        on bill_sponsors.bill_id = bill_info.bill_id
    where
        bill_sponsors.bioguide_id is not null
        and bill_info.congress is not null
    group by
        bill_sponsors.bioguide_id,
        bill_info.congress,
        bill_sponsors.sponsorship_type
)

-- Join the total counts back with the original data to get the counts by policy area
select
    {{dbt_utils.generate_surrogate_key(['bill_sponsors.bioguide_id', 'bill_info.policy_area_name', 'bill_sponsors.sponsorship_type', 'bill_info.congress'])}} as id,
    bill_sponsors.bioguide_id,
    bill_info.policy_area_name,
    bill_sponsors.sponsorship_type,
    bill_info.congress,
    total_bills.num_total_bills,
    count(distinct bill_info.bill_id) as num_bills_by_policy_area,
    count(distinct bill_info.bill_id)::float / total_bills.num_total_bills::float as pct_of_total_bills
from
    bill_sponsors
left join
    bill_info
    on bill_sponsors.bill_id = bill_info.bill_id
left join
    total_bills
    on bill_sponsors.bioguide_id = total_bills.bioguide_id
    and bill_info.congress = total_bills.congress
    and bill_sponsors.sponsorship_type = total_bills.sponsorship_type
where
    bill_sponsors.bioguide_id is not null
    and bill_info.congress is not null
    and bill_info.policy_area_name is not null
group by
    bill_sponsors.bioguide_id,
    bill_info.policy_area_name,
    bill_sponsors.sponsorship_type,
    bill_info.congress,
    total_bills.num_total_bills