{{ config(
    materialized='table'
) }}

select
    bioguide_id,
    is_current_member,
    rss_url,
    twitter,
    twitter_id,
    facebook,
    youtube,
    youtube_id,
    mastodon
from {{ ref('stg_legislator_social_media') }}