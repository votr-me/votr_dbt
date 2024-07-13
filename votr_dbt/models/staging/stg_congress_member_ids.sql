-- models/staging/stg_congress_member_ids.sql

with base as (
    select
        distinct
        bioguide_id,
        "currentMember"::bool as is_current_member,
        thomas_id::float::int::text as thomas_id,
        opensecrets_id,
        lis_id,
        cspan_id::float::int::text as cspan_id,
        govtrack_id::float::int::text as govtrack_id,
        votesmart_id::float::int::text as votesmart_id,
        ballotpedia_id,
        washington_post_id,
        icpsr_id::float::int::text as icpsr_id,
        wikipedia_id,
        string_to_array(trim(fec_ids), ',') as fec_ids_array  -- Convert the comma-separated string to an array
    from {{source("raw", 'congress_members')}}
),

unnested as (
    select
        bioguide_id,
        is_current_member,
        thomas_id,
        opensecrets_id,
        lis_id,
        cspan_id,
        govtrack_id,
        votesmart_id,
        ballotpedia_id,
        washington_post_id,
        icpsr_id,
        wikipedia_id,
        unnest(fec_ids_array) as fec_id
    from base
)

select * from unnested
