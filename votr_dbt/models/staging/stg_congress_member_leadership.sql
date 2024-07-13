-- models/staging/stg_congress_leadership.sql

with base as (
    select
        "bioguideId" as bioguide_id,
        congress::int::text as congress,
        current::bool as is_current,
        type as leadership_type
    from {{ source('raw', 'congress_leadership') }}
)

select * from base