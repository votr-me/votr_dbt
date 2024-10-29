{{ config(materialized='table') }}
with base as (
        select
        bioguide_id,
        current_member AS is_current_member,
        rss_url,
        twitter,
        CAST(twitter_id AS STRING) AS twitter_id,
        facebook,
        youtube,
        youtube_id,
        mastodon
    from {{ source('raw', 'legislators') }}
)

select * from base
