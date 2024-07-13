{{ config(
    materialized='table'
) }}

select 
    bioguide_id,
    bill_id
from {{ ref('stg_bill_cosponsors') }}