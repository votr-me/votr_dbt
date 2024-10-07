{{ config(
    materialized='table'
) }}

select
    bioguide_id,
    is_current_member,
    num_sponsored_legislation,
    sponsored_legislation_url,
    num_cosponsored_legislation,
    cosponsored_legislation_url
from {{ ref('stg_legislator_legislation_activity') }}
