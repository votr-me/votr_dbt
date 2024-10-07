{{ config(
    materialized='view'
) }}

SELECT *
FROM {{ ref('acs5_state_employment_historical') }}
WHERE
    year
    = (
        SELECT max(year) AS year
        FROM {{ ref('acs5_state_employment_historical') }}
    )
