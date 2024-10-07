{{ config(
    materialized='view'
) }}

SELECT *
FROM {{ ref('acs5_cd_employment_historical') }}
WHERE
    year
    = (SELECT max(year) AS year FROM {{ ref('acs5_cd_employment_historical') }})
