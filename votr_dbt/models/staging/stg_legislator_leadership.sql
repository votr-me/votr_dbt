-- models/staging/stg_congress_leadership.sql

{{ config(materialized='table') }}
with base as (
    select
        `bioguideId` AS bioguide_id,
        CAST(congress AS STRING) AS congress,  -- Cast directly to STRING
        CAST(`current` AS BOOL) AS is_current,
        type AS leadership_type
    from {{ source('raw', 'congress_leadership') }}
)

select * from base
