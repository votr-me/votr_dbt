-- File: models/staging/stg_legislators.sql

{{ config(materialized='incremental') }}

WITH base AS (

    SELECT
        bioguide_id,

        -- Cleaned text fields
        {{ clean_text('last_name') }} AS last_name,
        {{ clean_text('first_name') }} AS first_name,
        {{ clean_text('middle_name') }} AS middle_name,
        {{ clean_text('suffix') }} AS suffix,
        {{ clean_text('nickname') }} AS nickname,

        -- Simplify birthday handling
        NULLIF(trim(birthday), '')::DATE AS birthday,

        {{ clean_text('gender') }} AS gender,
        {{ clean_text('type') }} AS member_type,
        {{ clean_text('state', 'upper') }} AS member_state,
        LPAD(CAST(district AS VARCHAR), 2, '0') AS member_district,
        senate_class,
        {{ clean_text('party', 'upper') }} AS member_party,
        "currentMember"::BOOLEAN AS is_current_member,

        -- Simplify depiction_attribution extraction
        CASE
            WHEN "depiction_attribution" LIKE '%<a href=%</a>%'
                THEN regexp_replace("depiction_attribution", '.*<a href=.*">(.*)</a>.*', '\1')
            ELSE "depiction_attribution"
        END AS depiction_attribution,

        "depiction_imageUrl" AS depiction_image_url

    FROM {{ source('raw', 'legislators') }}

)

SELECT *
FROM base
