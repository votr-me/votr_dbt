-- File: models/staging/stg_legislators.sql

{{ config(materialized='incremental') }}

WITH base AS (

    SELECT
        bioguide_id,
        {{ clean_text('last_name') }} AS last_name,
        {{ clean_text('first_name') }} AS first_name,
        {{ clean_text('middle_name') }} AS middle_name,
        {{ clean_text('suffix') }} AS suffix,
        {{ clean_text('nickname') }} AS nickname,
        {{ clean_text('gender') }} AS gender,
        {{ clean_text('type') }} AS member_type,
        {{ clean_text('state', 'upper') }} AS member_state,
        {{ clean_text('party', 'upper') }} AS member_party,
        CAST(`currentMember` AS BOOL) AS is_current_member,
        CAST(NULLIF(TRIM(birthday), '') AS DATE) AS birthday,
        LPAD(CAST(district AS STRING), 2, '0') AS member_district,
        senate_class,
        CASE
            WHEN `depiction_attribution` LIKE '%<a href=%</a>%' THEN REGEXP_EXTRACT(`depiction_attribution`, r'.*<a href=.*">(.*)</a>.*')
            ELSE `depiction_attribution`
        END AS depiction_attribution,
        `depiction_imageUrl` AS depiction_image_url

    FROM {{ source('raw', 'legislators') }}

)

SELECT *
FROM base