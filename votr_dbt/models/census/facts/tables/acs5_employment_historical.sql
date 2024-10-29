{{ config(
    materialized='table',
    primary_key = 'id'
) }}

WITH cd_employment AS (
    SELECT
        year,
        state_fip,
        congressional_district,
        'us' AS country,
        employment_status_total AS cd_employment_status_total,
        in_labor_force AS cd_in_labor_force,
        pct_in_labor_force AS cd_pct_in_labor_force,
        not_in_labor_force AS cd_not_in_labor_force,
        pct_not_in_labor_force AS cd_pct_not_in_labor_force,
        civilian_labor_force AS cd_civilian_labor_force,
        pct_civilian_labor_force AS cd_pct_civilian_labor_force,
        civilian_employed AS cd_civilian_employed,
        pct_civilian_employed AS cd_pct_civilian_employed
    FROM
        {{ ref('acs5_cd_employment_historical') }}
    WHERE congressional_district != 'ZZ'
),

us_employment AS (
    SELECT
        year,
        employment_status_total AS us_employment_status_total,
        'us' AS country,
        in_labor_force AS us_in_labor_force,
        CAST(in_labor_force AS BIGNUMERIC) / CAST(employment_status_total AS BIGNUMERIC) AS us_pct_in_labor_force,
        not_in_labor_force AS us_not_in_labor_force,
        CAST(not_in_labor_force AS BIGNUMERIC) / CAST(employment_status_total AS BIGNUMERIC) AS us_pct_not_in_labor_force,
        civilian_labor_force AS us_civilian_labor_force,
        CAST(civilian_labor_force AS BIGNUMERIC) / CAST(employment_status_total AS BIGNUMERIC) AS us_pct_civilian_labor_force,
        civilian_employed AS us_civilian_employed,
        CAST(civilian_employed AS BIGNUMERIC) / CAST(employment_status_total AS BIGNUMERIC) AS us_pct_civilian_employed
    FROM
        {{ ref('acs5_us_employment_historical') }}
),

state_employment AS (
    SELECT
        year,
        state_fip,
        'us' AS country,
        employment_status_total AS state_employment_status_total,
        in_labor_force AS state_in_labor_force,
        CAST(in_labor_force AS BIGNUMERIC) / CAST(employment_status_total AS BIGNUMERIC) AS state_pct_in_labor_force,
        not_in_labor_force AS state_not_in_labor_force,
        CAST(not_in_labor_force AS BIGNUMERIC) / CAST(employment_status_total AS BIGNUMERIC) AS state_pct_not_in_labor_force,
        civilian_labor_force AS state_civilian_labor_force,
        CAST(civilian_labor_force AS BIGNUMERIC) / CAST(employment_status_total AS BIGNUMERIC) AS state_pct_civilian_labor_force,
        civilian_employed AS state_civilian_employed,
        CAST(civilian_employed AS BIGNUMERIC) / CAST(employment_status_total AS BIGNUMERIC) AS state_pct_civilian_employed
    FROM
        {{ ref('acs5_state_employment_historical') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['cd_employment.year', 'cd_employment.state_fip', 'cd_employment.congressional_district']) }} AS id,
    cd_employment.year,
    cd_employment.state_fip,
    cd_employment.congressional_district,
    cd_employment.country,
    cd_employment.cd_employment_status_total,
    cd_employment.cd_in_labor_force,
    cd_employment.cd_pct_in_labor_force,
    cd_employment.cd_not_in_labor_force,
    cd_employment.cd_pct_not_in_labor_force,
    cd_employment.cd_civilian_labor_force,
    cd_employment.cd_pct_civilian_labor_force,
    cd_employment.cd_civilian_employed,
    cd_employment.cd_pct_civilian_employed,
    state_employment.state_employment_status_total,
    state_employment.state_in_labor_force,
    state_employment.state_pct_in_labor_force,
    state_employment.state_not_in_labor_force,
    state_employment.state_pct_not_in_labor_force,
    state_employment.state_civilian_labor_force,
    state_employment.state_pct_civilian_labor_force,
    state_employment.state_civilian_employed,
    state_employment.state_pct_civilian_employed,
    us_employment.us_employment_status_total,
    us_employment.us_in_labor_force,
    us_employment.us_pct_in_labor_force,
    us_employment.us_not_in_labor_force,
    us_employment.us_pct_not_in_labor_force,
    us_employment.us_civilian_labor_force,
    us_employment.us_pct_civilian_labor_force,
    us_employment.us_civilian_employed,
    us_employment.us_pct_civilian_employed
FROM
    cd_employment
LEFT JOIN
    state_employment ON cd_employment.state_fip = state_employment.state_fip
    AND cd_employment.year = state_employment.year
LEFT JOIN
    us_employment ON cd_employment.country = us_employment.country
    AND cd_employment.year = us_employment.year
 -- Removed ORDER BY clause