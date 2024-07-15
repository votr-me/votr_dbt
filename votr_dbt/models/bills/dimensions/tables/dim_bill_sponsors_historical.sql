{{ config(
    materialized='table'
) }}

select
    bioguide_id,
    bill_id,
    is_by_request
from {{ ref('stg_bill_sponsors') }}