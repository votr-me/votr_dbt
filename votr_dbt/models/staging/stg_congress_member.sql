with base as (
    select
        bioguide_id,
        regexp_replace(trim(lower(last_name)), '[[:punct:]]', '') as last_name,
        regexp_replace(trim(lower(first_name)), '[[:punct:]]', '') as first_name,
        regexp_replace(trim(lower(middle_name)), '[[:punct:]]', '') as middle_name,
        regexp_replace(trim(lower(suffix)), '[[:punct:]]', '') as suffix,
        regexp_replace(trim(lower(nickname)), '[[:punct:]]', '') as nickname,
        case
            when trim(birthday) = '' then null
            else trim(birthday)::Date
        end as birthday,
        regexp_replace(trim(lower(gender)), '[[:punct:]]', '') as gender,
        regexp_replace(trim(lower(type)), '[[:punct:]]', '') as member_type,
        regexp_replace(trim(upper(state)), '[[:punct:]]', '') as member_state,
        district as member_district,
        senate_class as senate_class,
        regexp_replace(trim(upper(party)), '[[:punct:]]', '') as member_party,
        "currentMember"::bool as is_current_member,
        case
            when "depiction_attribution" like '%<a href=%</a>%' then
                regexp_replace(regexp_replace("depiction_attribution", '.*<a href=.*">', ''), '</a>.*', '')
            else "depiction_attribution"
        end as depiction_attribution,
        "depiction_imageUrl" as depiction_image_url
    from {{ source('raw', 'congress_members') }}
)

select * from base