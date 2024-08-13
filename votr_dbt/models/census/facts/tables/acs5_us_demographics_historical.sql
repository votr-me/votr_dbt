{{ config(
    materialized='table',
    primary_key='id'
) }}

SELECT
    {{dbt_utils.generate_surrogate_key(['year' ])}} as id,
    year,
    total_population,
    total_population_moe,
    sex_total,
    sex_total_moe,
    sex_male,
    sex_male_moe,
    sex_male::float / sex_total::float as pct_sex_male,
    (
        sex_male_18_to_19_moe +
        sex_male_20 +
        sex_male_20_moe +
        sex_male_21 +
        sex_male_21_moe +
        sex_male_22_to_24 +
        sex_male_22_to_24_moe +
        sex_male_25_to_29 +
        sex_male_25_to_29_moe +
        sex_male_30_to_34 +
        sex_male_30_to_34_moe +
        sex_male_35_to_39 +
        sex_male_35_to_39_moe +
        sex_male_40_to_44 +
        sex_male_40_to_44_moe +
        sex_male_45_to_49 +
        sex_male_45_to_49_moe +
        sex_male_50_to_54 +
        sex_male_50_to_54_moe +
        sex_male_55_to_59 +
        sex_male_55_to_59_moe +
        sex_male_60_to_61 +
        sex_male_60_to_61_moe +
        sex_male_62_to_64 +
        sex_male_62_to_64_moe +
        sex_male_65_to_66 +
        sex_male_65_to_66_moe +
        sex_male_67_to_69 +
        sex_male_67_to_69_moe +
        sex_male_70_to_74 +
        sex_male_70_to_74_moe +
        sex_male_75_to_79 +
        sex_male_75_to_79_moe +
        sex_male_80_to_84 +
        sex_male_80_to_84_moe +
        sex_male_85_and_over +
        sex_male_85_and_over_moe
    ) AS male_voting_age_population,
    (
        sex_male_18_to_19_moe +
        sex_male_20 +
        sex_male_20_moe +
        sex_male_21 +
        sex_male_21_moe +
        sex_male_22_to_24 +
        sex_male_22_to_24_moe +
        sex_male_25_to_29 +
        sex_male_25_to_29_moe +
        sex_male_30_to_34 +
        sex_male_30_to_34_moe +
        sex_male_35_to_39 +
        sex_male_35_to_39_moe +
        sex_male_40_to_44 +
        sex_male_40_to_44_moe +
        sex_male_45_to_49 +
        sex_male_45_to_49_moe +
        sex_male_50_to_54 +
        sex_male_50_to_54_moe +
        sex_male_55_to_59 +
        sex_male_55_to_59_moe +
        sex_male_60_to_61 +
        sex_male_60_to_61_moe +
        sex_male_62_to_64 +
        sex_male_62_to_64_moe +
        sex_male_65_to_66 +
        sex_male_65_to_66_moe +
        sex_male_67_to_69 +
        sex_male_67_to_69_moe +
        sex_male_70_to_74 +
        sex_male_70_to_74_moe +
        sex_male_75_to_79 +
        sex_male_75_to_79_moe +
        sex_male_80_to_84 +
        sex_male_80_to_84_moe +
        sex_male_85_and_over +
        sex_male_85_and_over_moe
    ) ::float / sex_total::float as pct_male_voting_age_population,
    sex_female,
    sex_female_moe,
    sex_female::float / sex_total::float as pct_sex_female,
    (
        sex_female_18_to_19_moe +
        sex_female_20 +
        sex_female_20_moe +
        sex_female_21 +
        sex_female_21_moe +
        sex_female_22_to_24 +
        sex_female_22_to_24_moe +
        sex_female_25_to_29 +
        sex_female_25_to_29_moe +
        sex_female_30_to_34 +
        sex_female_30_to_34_moe +
        sex_female_35_to_39 +
        sex_female_35_to_39_moe +
        sex_female_40_to_44 +
        sex_female_40_to_44_moe +
        sex_female_45_to_49 +
        sex_female_45_to_49_moe +
        sex_female_50_to_54 +
        sex_female_50_to_54_moe +
        sex_female_55_to_59 +
        sex_female_55_to_59_moe +
        sex_female_60_to_61 +
        sex_female_60_to_61_moe +
        sex_female_62_to_64 +
        sex_female_62_to_64_moe +
        sex_female_65_to_66 +
        sex_female_65_to_66_moe +
        sex_female_67_to_69 +
        sex_female_67_to_69_moe +
        sex_female_70_to_74 +
        sex_female_70_to_74_moe +
        sex_female_75_to_79 +
        sex_female_75_to_79_moe +
        sex_female_80_to_84 +
        sex_female_80_to_84_moe +
        sex_female_85_and_over +
        sex_female_85_and_over_moe
    ) AS female_voting_age_population,
    (
        sex_female_18_to_19_moe +
        sex_female_20 +
        sex_female_20_moe +
        sex_female_21 +
        sex_female_21_moe +
        sex_female_22_to_24 +
        sex_female_22_to_24_moe +
        sex_female_25_to_29 +
        sex_female_25_to_29_moe +
        sex_female_30_to_34 +
        sex_female_30_to_34_moe +
        sex_female_35_to_39 +
        sex_female_35_to_39_moe +
        sex_female_40_to_44 +
        sex_female_40_to_44_moe +
        sex_female_45_to_49 +
        sex_female_45_to_49_moe +
        sex_female_50_to_54 +
        sex_female_50_to_54_moe +
        sex_female_55_to_59 +
        sex_female_55_to_59_moe +
        sex_female_60_to_61 +
        sex_female_60_to_61_moe +
        sex_female_62_to_64 +
        sex_female_62_to_64_moe +
        sex_female_65_to_66 +
        sex_female_65_to_66_moe +
        sex_female_67_to_69 +
        sex_female_67_to_69_moe +
        sex_female_70_to_74 +
        sex_female_70_to_74_moe +
        sex_female_75_to_79 +
        sex_female_75_to_79_moe +
        sex_female_80_to_84 +
        sex_female_80_to_84_moe +
        sex_female_85_and_over +
        sex_female_85_and_over_moe
    )::float / sex_total::float AS pct_female_voting_age_population,
    race_white_total,
    race_white_total_moe,
    race_white_total::float /total_population::float as pct_race_white,
    race_black_total,
    race_black_total_moe,
    race_black_total::float /total_population::float as pct_race_black,
    race_asian_total,
    race_asian_total_moe,
    race_asian_total::float /total_population::float as pct_race_asian,
    race_pacific_islander_total,
    race_pacific_islander_total_moe,
    race_asian_total::float /total_population::float as pct_race_pacific_islande,
    race_other_total,
    race_other_total_moe,
    race_other_total::float /total_population::float as pct_race_other,
    race_two_or_more_total,
    race_two_or_more_total_moe,
    race_asian_total::float /total_population::float as pct_race_two_or_more,
    hispanic_total,
    hispanic_total_moe,
    race_asian_total::float /total_population::float as pct_race_hispanic
FROM {{ ref('stg_acs5_us') }}
