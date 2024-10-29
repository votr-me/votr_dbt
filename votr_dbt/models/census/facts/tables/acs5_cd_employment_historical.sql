{{ config(
    materialized='table',
    primary_key = 'id'
) }}

SELECT
    {{ dbt_utils.generate_surrogate_key(['year', 'state_fip', 'congressional_district']) }} as id,
    year,
    state_fip,
    congressional_district,
    employment_status_total,
    employment_status_total_moe,
    in_labor_force,
    in_labor_force_moe,
    CASE
        WHEN employment_status_total = 0 THEN NULL
        ELSE CAST(in_labor_force AS BIGNUMERIC) / CAST(employment_status_total AS BIGNUMERIC)
    END AS pct_in_labor_force,
    not_in_labor_force,
    not_in_labor_force_moe,
    CASE
        WHEN employment_status_total = 0 THEN NULL
        ELSE CAST(not_in_labor_force AS BIGNUMERIC) / CAST(employment_status_total AS BIGNUMERIC)
    END AS pct_not_in_labor_force,
    civilian_labor_force,
    civilian_labor_force_moe,
    CASE
        WHEN employment_status_total = 0 THEN NULL
        ELSE CAST(civilian_labor_force AS BIGNUMERIC) / CAST(employment_status_total AS BIGNUMERIC)
    END AS pct_civilian_labor_force,
    civilian_employed,
    civilian_employed_moe,
    CASE
        WHEN employment_status_total = 0 THEN NULL
        ELSE CAST(civilian_employed AS BIGNUMERIC) / CAST(employment_status_total AS BIGNUMERIC)
    END AS pct_civilian_employed,
    civilian_unemployed,
    civilian_unemployed_moe,
    CASE
        WHEN employment_status_total = 0 THEN NULL
        ELSE CAST(civilian_unemployed AS BIGNUMERIC) / CAST(employment_status_total AS BIGNUMERIC)
    END AS pct_civilian_unemployed,
    armed_forces,
    armed_forces_moe,
    CASE
        WHEN employment_status_total = 0 THEN NULL
        ELSE CAST(armed_forces AS BIGNUMERIC) / CAST(employment_status_total AS BIGNUMERIC)
    END AS pct_armed_forces_employed
FROM {{ ref('stg_acs5_congressional_districts') }}
