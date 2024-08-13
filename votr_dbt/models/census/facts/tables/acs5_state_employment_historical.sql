{{ config(
    materialized='table',
    primary_key='id'
) }}

SELECT
    {{dbt_utils.generate_surrogate_key(['year', 'state_fip'])}} as id,
    year,
    state_fip,
    employment_status_total,
    employment_status_total_moe,
    in_labor_force,
    in_labor_force_moe,
    CASE 
        WHEN employment_status_total = 0 THEN Null
        ELSE in_labor_force::float / employment_status_total 
    END as pct_in_labor_force,
    not_in_labor_force,
    not_in_labor_force_moe,
    CASE 
        WHEN employment_status_total = 0 THEN Null
        ELSE not_in_labor_force::float / employment_status_total 
    END as pct_not_in_labor_force,
    civilian_labor_force,
    civilian_labor_force_moe,
    CASE 
        WHEN employment_status_total = 0 THEN Null
        ELSE civilian_labor_force::float / employment_status_total::float 
    END as pct_civilian_labor_force,
    civilian_employed,
    civilian_employed_moe,
    CASE 
        WHEN employment_status_total = 0 THEN Null
        ELSE civilian_employed::float / employment_status_total::float 
    END as pct_civilian_employed,
    civilian_unemployed,
    civilian_unemployed_moe,
    CASE 
        WHEN employment_status_total = 0 THEN Null
        ELSE civilian_unemployed::float / employment_status_total::float 
    END as pct_civilian_unemployed,
    armed_forces,
    armed_forces_moe,
    CASE 
        WHEN employment_status_total = 0 THEN Null
        ELSE armed_forces::float / employment_status_total::float 
    END as pct_armed_forces_employed
FROM {{ ref('stg_acs5_states') }}
