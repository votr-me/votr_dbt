{{ config(
    materialized='view'
) }}

SELECT
    year,
    employment_status_total,
    employment_status_total_moe,
    in_labor_force,
    in_labor_force_moe,
    not_in_labor_force,
    not_in_labor_force_moe,
    civilian_labor_force,
    civilian_labor_force_moe,
    civilian_employed,
    civilian_employed_moe,
    civilian_unemployed,
    civilian_unemployed_moe,
    armed_forces,
    armed_forces_moe
FROM {{ ref('acs5_us_employment_historical') }}
WHERE year = (select max(year) as year from {{ ref('acs5_us_employment_historical') }})