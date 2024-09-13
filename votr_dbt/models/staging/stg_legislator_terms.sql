with member_terms as (
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
    from {{ source('raw', 'legislator_terms') }}
),

current_members as (
    select
        "bioguideId" as bioguide_id,
        "currentMember" as is_current_member
    from {{ source('raw', 'legislators') }}
)

select
    mt.bioguide_id,
    mt.chamber,
    mt.member_type,
    mt.congress,
    mt.state_code,
    mt.state_name,
    mt.district,
    mt.start_year,
    mt.end_year,
    cm.is_current_member
from member_terms mt
left join current_members cm
    on mt.bioguide_id = cm.bioguide_id