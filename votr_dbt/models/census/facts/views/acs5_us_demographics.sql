{{ config(
    materialized='view'
) }}

SELECT *
FROM {{ ref('acs5_us_demographics_historical') }}
WHERE
    year
    = (
        SELECT max(year) AS year
        FROM {{ ref('acs5_us_demographics_historical') }}
    )
