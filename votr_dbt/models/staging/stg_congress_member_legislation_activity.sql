
select bioguide_id,
    "currentMember"::bool as is_current_member,
    "sponsoredLegislation_count"::float::int as num_sponsored_legislation,
    "sponsoredLegislation_url" as sponsored_legislation_url,
    "cosponsoredLegislation_count"::float::int as num_consponsored_legislation,
    "cosponsoredLegislation_url" as cosponsored_legislation_url
from {{ source('raw', 'congress_members') }}