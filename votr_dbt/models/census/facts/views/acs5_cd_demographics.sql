{{ config(
    materialized='view'
) }}

SELECT *
FROM {{ ref('acs5_cd_demographics_historical') }}
WHERE year = (select max(year) as year from {{ ref('acs5_cd_demographics_historical') }})