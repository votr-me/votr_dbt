{{ config(materialized='incremental') }}
select
    bioguide_id,
    CAST(`currentMember` AS BOOL) AS is_current_member,
    CAST(`sponsoredLegislation_count` AS INT64) AS num_sponsored_legislation,
    `sponsoredLegislation_url` AS sponsored_legislation_url,
    CAST(`cosponsoredLegislation_count` AS INT64) AS num_cosponsored_legislation,
    `cosponsoredLegislation_url` AS cosponsored_legislation_url
from {{ source('raw', 'legislators') }}
