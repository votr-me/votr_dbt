{{ config(
    materialized='view'
) }}

SELECT
    year,
    state_fip,
total_population,
    total_population_moe,
    sex_total,
    sex_total_moe,
    sex_male,
    sex_male_moe,
    male_voting_age_population,
    sex_female,
    sex_female_moe,
    female_voting_age_population,
    race_white_total,
    race_white_total_moe,
    race_black_total,
    race_black_total_moe,
    race_asian_total,
    race_asian_total_moe,
    race_pacific_islander_total,
    race_pacific_islander_total_moe,
    race_other_total,
    race_other_total_moe,
    race_two_or_more_total,
    race_two_or_more_total_moe,
    hispanic_total,
    hispanic_total_moe
FROM {{ ref('acs5_state_demographics_historical') }}
WHERE year = (select max(year) as year from {{ ref('acs5_state_demographics_historical') }})