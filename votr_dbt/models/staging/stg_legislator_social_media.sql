with base as (
    select
        bioguide_id,
        current_member as is_current_member,
        rss_url,
        twitter,
        twitter_id::float::BIGINT::text as twitter_id,
        facebook,
        youtube,
        youtube_id,
        mastodon
    from {{ source('raw', 'legislators') }}
)

select * from base