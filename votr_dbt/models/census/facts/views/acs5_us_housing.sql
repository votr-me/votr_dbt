{{ config(
    materialized='view'
) }}

SELECT *
FROM {{ ref('acs5_us_housing_historical') }}
WHERE year = (select max(year) as year from {{ ref('acs5_us_housing_historical') }})