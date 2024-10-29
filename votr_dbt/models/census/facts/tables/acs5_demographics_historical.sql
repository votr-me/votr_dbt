{{ config(
    materialized='table',
    primary_key = 'id'
) }}

WITH cd_demographics AS (
    SELECT
        year,
        congressional_district,
        state_fip,
        'us' AS country,
        'cd' AS record_level,
        sex_total AS cd_sex_total,
        CAST(sex_male AS BIGNUMERIC) / CAST(sex_total AS BIGNUMERIC) AS cd_pct_male,
        sex_male AS cd_sex_male,
        CAST(sex_female AS BIGNUMERIC) / CAST(sex_total AS BIGNUMERIC) AS cd_pct_female,
        (CAST(male_voting_age_population AS BIGNUMERIC) + CAST(female_voting_age_population AS BIGNUMERIC)) AS cd_total_voting_age,
        (CAST(male_voting_age_population AS BIGNUMERIC) + CAST(female_voting_age_population AS BIGNUMERIC)) / CAST(sex_total AS BIGNUMERIC) AS cd_pct_population_of_voting_age,
        race_white_total AS cd_race_white_total,
        CAST(race_white_total AS BIGNUMERIC) / CAST(total_population AS BIGNUMERIC) AS cd_pct_white,
        race_asian_total AS cd_asian_total,
        CAST(race_asian_total AS BIGNUMERIC) / CAST(total_population AS BIGNUMERIC) AS cd_pct_asian,
        race_black_total AS cd_race_black_total,
        CAST(race_black_total AS BIGNUMERIC) / CAST(total_population AS BIGNUMERIC) AS cd_pct_black,
        hispanic_total AS cd_race_hispanic_total,
        CAST(hispanic_total AS BIGNUMERIC) / CAST(total_population AS BIGNUMERIC) AS cd_pct_hispanic,
        race_pacific_islander_total AS cd_race_pacific_islander_total,
        CAST(race_pacific_islander_total AS BIGNUMERIC) / CAST(total_population AS BIGNUMERIC) AS cd_pct_pacific_islander,
        race_other_total AS cd_race_other_total,
        CAST(race_other_total AS BIGNUMERIC) / CAST(total_population AS BIGNUMERIC) AS cd_pct_other_race,
        race_two_or_more_total AS cd_race_two_or_more_total,
        CAST(race_two_or_more_total AS BIGNUMERIC) / CAST(total_population AS BIGNUMERIC) AS cd_pct_two_or_more
    FROM
        {{ ref('acs5_cd_demographics_historical') }}
    WHERE congressional_district NOT LIKE 'ZZ'
),

state_demographics AS (
    SELECT
        year,
        state_fip,
        'us' AS country,
        'state' AS record_level,
        sex_total AS state_sex_total,
        CAST(sex_male AS BIGNUMERIC) / CAST(sex_total AS BIGNUMERIC) AS state_pct_male,
        sex_male AS state_sex_male,
        CAST(sex_female AS BIGNUMERIC) / CAST(sex_total AS BIGNUMERIC) AS state_pct_female,
        (CAST(male_voting_age_population AS BIGNUMERIC) + CAST(female_voting_age_population AS BIGNUMERIC)) AS state_total_voting_age,
        (CAST(male_voting_age_population AS BIGNUMERIC) + CAST(female_voting_age_population AS BIGNUMERIC)) / CAST(sex_total AS BIGNUMERIC) AS state_pct_population_of_voting_age,
        race_white_total AS state_race_white_total,
        CAST(race_white_total AS BIGNUMERIC) / CAST(total_population AS BIGNUMERIC) AS state_pct_white,
        race_asian_total AS state_asian_total,
        CAST(race_asian_total AS BIGNUMERIC) / CAST(total_population AS BIGNUMERIC) AS state_pct_asian,
        race_black_total AS state_race_black_total,
        CAST(race_black_total AS BIGNUMERIC) / CAST(total_population AS BIGNUMERIC) AS state_pct_black,
        hispanic_total AS state_race_hispanic_total,
        CAST(hispanic_total AS BIGNUMERIC) / CAST(total_population AS BIGNUMERIC) AS state_pct_hispanic,
        race_pacific_islander_total AS state_race_pacific_islander_total,
        CAST(race_pacific_islander_total AS BIGNUMERIC) / CAST(total_population AS BIGNUMERIC) AS state_pct_pacific_islander,
        race_other_total AS state_race_other_total,
        CAST(race_other_total AS BIGNUMERIC) / CAST(total_population AS BIGNUMERIC) AS state_pct_other_race,
        race_two_or_more_total AS state_race_two_or_more_total,
        CAST(race_two_or_more_total AS BIGNUMERIC) / CAST(total_population AS BIGNUMERIC) AS state_pct_two_or_more
    FROM
        {{ ref('acs5_state_demographics_historical') }}
),

us_demographics AS (
    SELECT
        year,
        'us' AS country,
        sex_total AS us_sex_total,
        CAST(sex_male AS BIGNUMERIC) / CAST(sex_total AS BIGNUMERIC) AS us_pct_male,
        sex_male AS us_sex_male,
        CAST(sex_female AS BIGNUMERIC) / CAST(sex_total AS BIGNUMERIC) AS us_pct_female,
        (CAST(male_voting_age_population AS BIGNUMERIC) + CAST(female_voting_age_population AS BIGNUMERIC)) AS us_total_voting_age,
        (CAST(male_voting_age_population AS BIGNUMERIC) + CAST(female_voting_age_population AS BIGNUMERIC)) / CAST(sex_total AS BIGNUMERIC) AS us_pct_population_of_voting_age,
        race_white_total AS us_race_white_total,
        CAST(race_white_total AS BIGNUMERIC) / CAST(total_population AS BIGNUMERIC) AS us_pct_white,
        race_asian_total AS us_asian_total,
        CAST(race_asian_total AS BIGNUMERIC) / CAST(total_population AS BIGNUMERIC) AS us_pct_asian,
        race_black_total AS us_race_black_total,
        CAST(race_black_total AS BIGNUMERIC) / CAST(total_population AS BIGNUMERIC) AS us_pct_black,
        hispanic_total AS us_race_hispanic_total,
        CAST(hispanic_total AS BIGNUMERIC) / CAST(total_population AS BIGNUMERIC) AS us_pct_hispanic,
        race_pacific_islander_total AS us_race_pacific_islander_total,
        CAST(race_pacific_islander_total AS BIGNUMERIC) / CAST(total_population AS BIGNUMERIC) AS us_pct_pacific_islander,
        race_other_total AS us_race_other_total,
        CAST(race_other_total AS BIGNUMERIC) / CAST(total_population AS BIGNUMERIC) AS us_pct_other_race,
        race_two_or_more_total AS us_race_two_or_more_total,
        CAST(race_two_or_more_total AS BIGNUMERIC) / CAST(total_population AS BIGNUMERIC) AS us_pct_two_or_more
    FROM
        {{ ref('acs5_us_demographics_historical') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['cd_demographics.year', 'cd_demographics.state_fip', 'cd_demographics.congressional_district']) }} AS id,
    cd_demographics.year,
    cd_demographics.state_fip,
    cd_demographics.congressional_district,
    cd_demographics.country,
    cd_demographics.cd_sex_total,
    cd_demographics.cd_pct_male,
    cd_demographics.cd_sex_male,
    cd_demographics.cd_pct_female,
    cd_demographics.cd_total_voting_age,
    cd_demographics.cd_pct_population_of_voting_age,
    cd_demographics.cd_race_white_total,
    cd_demographics.cd_pct_white,
    cd_demographics.cd_asian_total,
    cd_demographics.cd_pct_asian,
    cd_demographics.cd_race_black_total,
    cd_demographics.cd_pct_black,
    cd_demographics.cd_race_hispanic_total,
    cd_demographics.cd_pct_hispanic,
    cd_demographics.cd_race_pacific_islander_total,
    cd_demographics.cd_pct_pacific_islander,
    cd_demographics.cd_race_other_total,
    cd_demographics.cd_pct_other_race,
    cd_demographics.cd_race_two_or_more_total,
    cd_demographics.cd_pct_two_or_more,
    state_demographics.state_sex_total,
    state_demographics.state_pct_male,
    state_demographics.state_sex_male,
    state_demographics.state_pct_female,
    state_demographics.state_total_voting_age,
    state_demographics.state_pct_population_of_voting_age,
    state_demographics.state_race_white_total,
    state_demographics.state_pct_white,
    state_demographics.state_asian_total,
    state_demographics.state_pct_asian,
    state_demographics.state_race_black_total,
    state_demographics.state_pct_black,
    state_demographics.state_race_hispanic_total,
    state_demographics.state_pct_hispanic,
    state_demographics.state_race_pacific_islander_total,
    state_demographics.state_pct_pacific_islander,
    state_demographics.state_race_other_total,
    state_demographics.state_pct_other_race,
    state_demographics.state_race_two_or_more_total,
    state_demographics.state_pct_two_or_more,
    us_demographics.us_sex_total,
    us_demographics.us_pct_male,
    us_demographics.us_sex_male,
    us_demographics.us_pct_female,
    us_demographics.us_total_voting_age,
    us_demographics.us_pct_population_of_voting_age,
    us_demographics.us_race_white_total,
    us_demographics.us_pct_white,
    us_demographics.us_asian_total,
    us_demographics.us_pct_asian,
    us_demographics.us_race_black_total,
    us_demographics.us_pct_black,
    us_demographics.us_race_hispanic_total,
    us_demographics.us_pct_hispanic,
    us_demographics.us_race_pacific_islander_total,
    us_demographics.us_pct_pacific_islander,
    us_demographics.us_race_other_total,
    us_demographics.us_pct_other_race,
    us_demographics.us_race_two_or_more_total,
    us_demographics.us_pct_two_or_more
FROM
    cd_demographics
LEFT JOIN
    state_demographics ON cd_demographics.state_fip = state_demographics.state_fip
        AND cd_demographics.year = state_demographics.year
LEFT JOIN
    us_demographics ON cd_demographics.country = us_demographics.country
        AND cd_demographics.year = us_demographics.year