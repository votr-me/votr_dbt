{{ config(materialized='table') }}
select
    bioguide_id,
    bill_id
from {{ source('raw', 'bill_cosponsors') }}
