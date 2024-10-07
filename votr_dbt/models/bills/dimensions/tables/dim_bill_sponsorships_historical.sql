{{ config(
    materialized='table'
) }}

with sponsors as (
    select
        bioguide_id,
        bill_id
    from {{ ref('stg_bill_sponsors') }}
),

cosponsors as (
    select
        bioguide_id,
        bill_id
    from {{ ref('stg_bill_cosponsors') }}
)

select
    bioguide_id,
    bill_id,
    'cosponsor' as sponsorship_type
from cosponsors
union all
select
    bioguide_id,
    bill_id,
    'sponsor' as sponsorship_type
from sponsors
