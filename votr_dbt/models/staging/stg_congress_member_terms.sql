select
    "bioguideId" as bioguide_id,
    chamber,
    "memberType" as member_type,
    congress::int::text as congress,
    "stateCode" as state_code,
    "stateName" as state_name,
    district::float::int::text as district,
    "startYear"::float::int as start_year,
    "endYear"::float::int as end_year
from {{ source('raw', 'congress_member_terms') }}