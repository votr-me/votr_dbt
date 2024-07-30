{{ config(
    materialized='table'
) }}

SELECT
    year,
    tenure_total,
    tenure_total_moe,
    owner_occupied,
    owner_occupied_moe,
    renter_occupied,
    renter_occupied_moe,
    median_contract_rent,
    median_contract_rent_moe,
    rent_asked_total,
    rent_asked_total_moe,
    median_gross_rent,
    median_gross_rent_moe
FROM {{ ref('stg_acs5_us') }}