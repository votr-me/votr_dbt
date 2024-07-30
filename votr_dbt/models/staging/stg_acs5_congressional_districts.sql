{{ config(
    materialized='table'
) }}

WITH source AS (
    SELECT
        "year" as year,
        "state" as state_fip,
        "congressional_district" as congressional_district,
        "GEO_ID" as geo_id, 
        "B01001_001E"::FLOAT::BIGINT AS sex_total_population,
        "B01001_001M"::FLOAT::BIGINT AS sex_moe,
        "B01001_002E"::FLOAT::BIGINT AS sex_male,
        "B01001_002M"::FLOAT::BIGINT AS sex_male_moe,
        "B01001_003E"::FLOAT::BIGINT AS sex_male_under_5,
        "B01001_003M"::FLOAT::BIGINT AS sex_male_under_5_moe,
        "B01001_004E"::FLOAT::BIGINT AS sex_male_5_to_9,
        "B01001_004M"::FLOAT::BIGINT AS sex_male_5_to_9_moe,
        "B01001_005E"::FLOAT::BIGINT AS sex_male_10_to_14,
        "B01001_005M"::FLOAT::BIGINT AS sex_male_10_to_14_moe,
        "B01001_006E"::FLOAT::BIGINT AS sex_male_15_to_17,
        "B01001_006M"::FLOAT::BIGINT AS sex_male_15_to_17_moe,
        "B01001_026E"::FLOAT::BIGINT AS sex_female,
        "B01001_026M"::FLOAT::BIGINT AS sex_female_moe,
        "B01001_027E"::FLOAT::BIGINT AS sex_female_under_5,
        "B01001_027M"::FLOAT::BIGINT AS sex_female_under_5_moe,
        "B01001_028E"::FLOAT::BIGINT AS sex_female_5_to_9,
        "B01001_028M"::FLOAT::BIGINT AS sex_female_5_to_9_moe,
        "B01001_029E"::FLOAT::BIGINT AS sex_female_10_to_14,
        "B01001_029M"::FLOAT::BIGINT AS sex_female_10_to_14_moe,
        "B01001_030E"::FLOAT::BIGINT AS sex_female_15_to_17,
        "B01001_030M"::FLOAT::BIGINT AS sex_female_15_to_17_moe,
        "B01003_001E"::FLOAT::BIGINT AS total_population,
        "B01003_001M"::FLOAT::BIGINT AS total_population_moe,
        "B02001_002E"::FLOAT::BIGINT AS race_white_alone,
        "B02001_002M"::FLOAT::BIGINT AS race_white_alone_moe,
        "B02001_003E"::FLOAT::BIGINT AS race_black_or_african_american_alone,
        "B02001_003M"::FLOAT::BIGINT AS race_black_or_african_american_alone_moe,
        "B02001_004E"::FLOAT::BIGINT AS race_american_indian_and_alaska_native_alone,
        "B02001_004M"::FLOAT::BIGINT AS race_american_indian_and_alaska_native_alone_moe,
        "B02001_005E"::FLOAT::BIGINT AS race_asian_alone,
        "B02001_005M"::FLOAT::BIGINT AS race_asian_alone_moe,
        "B02001_006E"::FLOAT::BIGINT AS race_native_hawaiian_and_pacific_islander_alone,
        "B02001_006M"::FLOAT::BIGINT AS race_native_hawaiian_and_pacific_islander_alone_moe,
        "B02001_007E"::FLOAT::BIGINT AS race_other_race_alone,
        "B02001_007M"::FLOAT::BIGINT AS race_other_race_alone_moe,
        "B02001_008E"::FLOAT::BIGINT AS race_two_or_more_races,
        "B02001_008M"::FLOAT::BIGINT AS race_two_or_more_races_moe,
        "B03001_003E"::FLOAT::BIGINT AS hispanic_latino_total,
        "B03001_003M"::FLOAT::BIGINT AS hispanic_latino_total_moe,
        "B11001_001E"::FLOAT::BIGINT AS hh_total_population,
        "B11001_001M"::FLOAT::BIGINT AS hh_total_population_moe,
        "B11001_002E"::FLOAT::BIGINT AS hh_family,
        "B11001_002M"::FLOAT::BIGINT AS hh_family_moe,
        "B11001_003E"::FLOAT::BIGINT AS hh_family_married,
        "B11001_003M"::FLOAT::BIGINT AS hh_family_married_moe,
        "B11001_004E"::FLOAT::BIGINT AS hh_family_other,
        "B11001_004M"::FLOAT::BIGINT AS hh_family_other_moe,
        "B11001_005E"::FLOAT::BIGINT AS hh_family_male_no_spouse,
        "B11001_005M"::FLOAT::BIGINT AS hh_family_male_no_spouse_moe,
        "B11001_006E"::FLOAT::BIGINT AS hh_family_female_no_husband,
        "B11001_006M"::FLOAT::BIGINT AS hh_family_female_no_husband_moe,
        "B11001_007E"::FLOAT::BIGINT AS hh_nonfamily,
        "B11001_007M"::FLOAT::BIGINT AS hh_nonfamily_moe,
        "B11001_008E"::FLOAT::BIGINT AS hh_nonfamily_living_alone,
        "B11001_008M"::FLOAT::BIGINT AS hh_nonfamily_living_alone_moe,
        "B11001_009E"::FLOAT::BIGINT AS hh_nonfamily_not_living_alone,
        "B11001_009M"::FLOAT::BIGINT AS hh_nonfamily_not_living_alone_moe,
        "B11005_001E"::FLOAT::BIGINT AS hh_under_18_total,
        "B11005_001M"::FLOAT::BIGINT AS hh_under_18_total_moe,
        "B11005_002E"::FLOAT::BIGINT AS hh_under_18,
        "B11005_002M"::FLOAT::BIGINT AS hh_under_18_moe,
        "B11005_003E"::FLOAT::BIGINT AS hh_under_18_family,
        "B11005_003M"::FLOAT::BIGINT AS hh_under_18_family_moe,
        "B11005_004E"::FLOAT::BIGINT AS hh_under_18_married,
        "B11005_004M"::FLOAT::BIGINT AS hh_under_18_married_moe,
        "B11005_005E"::FLOAT::BIGINT AS hh_under_18_other,
        "B11005_005M"::FLOAT::BIGINT AS hh_under_18_other_moe,
        "B11005_006E"::FLOAT::BIGINT AS hh_under_18_male_no_spouse,
        "B11005_006M"::FLOAT::BIGINT AS hh_under_18_male_no_spouse_moe,
        "B11005_007E"::FLOAT::BIGINT AS hh_under_18_female_no_husband,
        "B11005_007M"::FLOAT::BIGINT AS hh_under_18_female_no_husband_moe,
        "B11005_008E"::FLOAT::BIGINT AS hh_under_18_nonfamily,
        "B11005_008M"::FLOAT::BIGINT AS hh_under_18_nonfamily_moe,
        "B11005_009E"::FLOAT::BIGINT AS hh_under_18_nonfamily_male,
        "B11005_009M"::FLOAT::BIGINT AS hh_under_18_nonfamily_male_moe,
        "B11005_010E"::FLOAT::BIGINT AS hh_under_18_nonfamily_female,
        "B11005_010M"::FLOAT::BIGINT AS hh_under_18_nonfamily_female_moe,
        "B11005_011E"::FLOAT::BIGINT AS hh_no_under_18,
        "B11005_011M"::FLOAT::BIGINT AS hh_no_under_18_moe,
        "B11005_012E"::FLOAT::BIGINT AS hh_no_under_18_family,
        "B11005_012M"::FLOAT::BIGINT AS hh_no_under_18_family_moe,
        "B11005_013E"::FLOAT::BIGINT AS hh_no_under_18_married,
        "B11005_013M"::FLOAT::BIGINT AS hh_no_under_18_married_moe,
        "B11005_014E"::FLOAT::BIGINT AS hh_no_under_18_other,
        "B11005_014M"::FLOAT::BIGINT AS hh_no_under_18_other_moe,
        "B11005_015E"::FLOAT::BIGINT AS hh_no_under_18_male_no_wife,
        "B11005_015M"::FLOAT::BIGINT AS hh_no_under_18_male_no_wife_moe,
        "B11005_016E"::FLOAT::BIGINT AS hh_no_under_18_female_no_spouse,
        "B11005_016M"::FLOAT::BIGINT AS hh_no_under_18_female_no_spouse_moe,
        "B11005_017E"::FLOAT::BIGINT AS hh_no_under_18_nonfamily,
        "B11005_017M"::FLOAT::BIGINT AS hh_no_under_18_nonfamily_moe,
        "B11005_018E"::FLOAT::BIGINT AS hh_no_under_18_nonfamily_male,
        "B11005_018M"::FLOAT::BIGINT AS hh_no_under_18_nonfamily_male_moe,
        "B11005_019E"::FLOAT::BIGINT AS hh_no_under_18_nonfamily_female,
        "B11005_019M"::FLOAT::BIGINT AS hh_no_under_18_nonfamily_female_moe,
        "B15003_001E"::FLOAT::BIGINT AS educational_attainment_total,
        "B15003_001M"::FLOAT::BIGINT AS educational_attainment_total_moe,
        "B15003_017E"::FLOAT::BIGINT AS educational_attainment_high_school,
        "B15003_017M"::FLOAT::BIGINT AS educational_attainment_high_school_moe,
        "B15003_022E"::FLOAT::BIGINT AS educational_attainment_bachelor_degree,
        "B15003_022M"::FLOAT::BIGINT AS educational_attainment_bachelor_degree_moe,
        "B15003_023E"::FLOAT::BIGINT AS educational_attainment_masters_degree,
        "B15003_023M"::FLOAT::BIGINT AS educational_attainment_masters_degree_moe,
        "B15003_024E"::FLOAT::BIGINT AS educational_attainment_professional_school,
        "B15003_024M"::FLOAT::BIGINT AS educational_attainment_professional_school_moe,
        "B15003_025E"::FLOAT::BIGINT AS educational_attainment_doctorate_degree,
        "B15003_025M"::FLOAT::BIGINT AS educational_attainment_doctorate_degree_moe,
        "B16001_001E"::FLOAT::BIGINT AS language_spoken_at_home_total,
        "B16001_001M"::FLOAT::BIGINT AS language_spoken_at_home_total_moe,
        "B16001_002E"::FLOAT::BIGINT AS language_spoken_at_home_english_only,
        "B16001_002M"::FLOAT::BIGINT AS language_spoken_at_home_english_only_moe,
        "B16001_003E"::FLOAT::BIGINT AS language_spoken_at_home_spanish,
        "B16001_003M"::FLOAT::BIGINT AS language_spoken_at_home_spanish_moe,
        "B16001_004E"::FLOAT::BIGINT AS language_spoken_at_home_spanish_english_very_well,
        "B16001_004M"::FLOAT::BIGINT AS language_spoken_at_home_spanish_english_very_well_moe,
        "B16001_005E"::FLOAT::BIGINT AS language_spoken_at_home_spanish_less_english_very_well,
        "B16001_005M"::FLOAT::BIGINT AS language_spoken_at_home_spanish_less_english_very_well_moe,
        "B16001_006E"::FLOAT::BIGINT AS language_spoken_at_home_french_cajun,
        "B16001_006M"::FLOAT::BIGINT AS language_spoken_at_home_french_cajun_moe,
        "B17001_001E"::FLOAT::BIGINT AS poverty_status_total,
        "B17001_001M"::FLOAT::BIGINT AS poverty_status_total_moe,
        "B17001_002E"::FLOAT::BIGINT AS poverty_status_income_below_poverty,
        "B17001_002M"::FLOAT::BIGINT AS poverty_status_income_below_poverty_moe,
        "B17001_003E"::FLOAT::BIGINT AS poverty_status_male_below_poverty,
        "B17001_003M"::FLOAT::BIGINT AS poverty_status_male_below_poverty_moe,
        "B17001_004E"::FLOAT::BIGINT AS poverty_status_male_under_5_below_poverty,
        "B17001_004M"::FLOAT::BIGINT AS poverty_status_male_under_5_below_poverty_moe,
        "B17001_005E"::FLOAT::BIGINT AS poverty_status_male_5_below_poverty,
        "B17001_005M"::FLOAT::BIGINT AS poverty_status_male_5_below_poverty_moe,
        "B17001_006E"::FLOAT::BIGINT AS poverty_status_male_6_to_11_below_poverty,
        "B17001_006M"::FLOAT::BIGINT AS poverty_status_male_6_to_11_below_poverty_moe,
        "B17001_007E"::FLOAT::BIGINT AS poverty_status_male_12_to_14_below_poverty,
        "B17001_007M"::FLOAT::BIGINT AS poverty_status_male_12_to_14_below_poverty_moe,
        "B17001_008E"::FLOAT::BIGINT AS poverty_status_male_15_below_poverty,
        "B17001_008M"::FLOAT::BIGINT AS poverty_status_male_15_below_poverty_moe,
        "B17001_009E"::FLOAT::BIGINT AS poverty_status_male_16_to_17_below_poverty,
        "B17001_009M"::FLOAT::BIGINT AS poverty_status_male_16_to_17_below_poverty_moe,
        "B18101_001E"::FLOAT::BIGINT AS disability_status_total,
        "B18101_001M"::FLOAT::BIGINT AS disability_status_total_moe,
        "B18101_002E"::FLOAT::BIGINT AS disability_status_male,
        "B18101_002M"::FLOAT::BIGINT AS disability_status_male_moe,
        "B18101_003E"::FLOAT::BIGINT AS disability_status_male_under_5,
        "B18101_003M"::FLOAT::BIGINT AS disability_status_male_under_5_moe,
        "B18101_004E"::FLOAT::BIGINT AS disability_status_male_under_5_with_disability,
        "B18101_004M"::FLOAT::BIGINT AS disability_status_male_under_5_with_disability_moe,
        "B18101_005E"::FLOAT::BIGINT AS disability_status_male_under_5_no_disability,
        "B18101_005M"::FLOAT::BIGINT AS disability_status_male_under_5_no_disability_moe,
        "B18101_006E"::FLOAT::BIGINT AS disability_status_male_5_to_17,
        "B18101_006M"::FLOAT::BIGINT AS disability_status_male_5_to_17_moe,
        "B18101_007E"::FLOAT::BIGINT AS disability_status_male_5_to_17_with_disability,
        "B18101_007M"::FLOAT::BIGINT AS disability_status_male_5_to_17_with_disability_moe,
        "B18101_008E"::FLOAT::BIGINT AS disability_status_male_5_to_17_no_disability,
        "B18101_008M"::FLOAT::BIGINT AS disability_status_male_5_to_17_no_disability_moe,
        "B18101_009E"::FLOAT::BIGINT AS disability_status_male_18_to_34,
        "B18101_009M"::FLOAT::BIGINT AS disability_status_male_18_to_34_moe,
        "B19001_001E"::FLOAT::BIGINT AS hh_income_total,
        "B19001_001M"::FLOAT::BIGINT AS hh_income_total_moe,
        "B19001_002E"::FLOAT::BIGINT AS hh_income_less_10k,
        "B19001_002M"::FLOAT::BIGINT AS hh_income_less_10k_moe,
        "B19001_003E"::FLOAT::BIGINT AS hh_income_10k_to_14K,
        "B19001_003M"::FLOAT::BIGINT AS hh_income_10k_to_14K_moe,
        "B19001_004E"::FLOAT::BIGINT AS hh_income_15k_to_19k,
        "B19001_004M"::FLOAT::BIGINT AS hh_income_15k_to_19k_moe,
        "B19001_005E"::FLOAT::BIGINT AS hh_income_20k_to_24k,
        "B19001_005M"::FLOAT::BIGINT AS hh_income_20k_to_24k_moe,
        "B19001_006E"::FLOAT::BIGINT AS hh_income_25k_to_29k,
        "B19001_006M"::FLOAT::BIGINT AS hh_income_25k_to_29k_moe,
        "B19001_007E"::FLOAT::BIGINT AS hh_income_30k_to_34k,
        "B19001_007M"::FLOAT::BIGINT AS hh_income_30k_to_34k_moe,
        "B19001_008E"::FLOAT::BIGINT AS hh_income_35k_to_39k,
        "B19001_008M"::FLOAT::BIGINT AS hh_income_35k_to_39k_moe,
        "B19001_009E"::FLOAT::BIGINT AS hh_income_40k_to_44k,
        "B19001_009M"::FLOAT::BIGINT AS hh_income_40k_to_44k_moe,
        "B19001_010E"::FLOAT::BIGINT AS hh_income_45k_to_49k,
        "B19001_010M"::FLOAT::BIGINT AS hh_income_45k_to_49k_moe,
        "B19001_011E"::FLOAT::BIGINT AS hh_income_50k_to_59k,
        "B19001_011M"::FLOAT::BIGINT AS hh_income_50k_to_59k_moe,
        "B19001_012E"::FLOAT::BIGINT AS hh_income_60k_to_74k,
        "B19001_012M"::FLOAT::BIGINT AS hh_income_60k_to_74k_moe,
        "B19001_013E"::FLOAT::BIGINT AS hh_income_75k_to_99k,
        "B19001_013M"::FLOAT::BIGINT AS hh_income_75k_to_99k_moe,
        "B19001_014E"::FLOAT::BIGINT AS hh_income_100k_to_124k,
        "B19001_014M"::FLOAT::BIGINT AS hh_income_100k_to_124k_moe,
        "B19001_015E"::FLOAT::BIGINT AS hh_income_125k_to_149k,
        "B19001_015M"::FLOAT::BIGINT AS hh_income_125k_to_149k_moe,
        "B19001_016E"::FLOAT::BIGINT AS hh_income_150k_to_199k,
        "B19001_016M"::FLOAT::BIGINT AS hh_income_150k_to_199k_moe,
        "B19001_017E"::FLOAT::BIGINT AS hh_income_200k_or_more,
        "B19001_017M"::FLOAT::BIGINT AS hh_income_200k_or_more_moe,
        "B19013_001E"::FLOAT::BIGINT AS median_hh_income_2015,
        "B19013_001M"::FLOAT::BIGINT AS median_hh_income_2015_moe,
        "B19025_001E"::FLOAT::BIGINT AS aggregate_hh_income_2011,
        "B19025_001M"::FLOAT::BIGINT AS aggregate_hh_income_2011_moe,
        "B19055_001E"::FLOAT::BIGINT AS social_security_income_total,
        "B19055_001M"::FLOAT::BIGINT AS social_security_income_total_moe,
        "B19056_001E"::FLOAT::BIGINT AS ssi_income_total,
        "B19056_001M"::FLOAT::BIGINT AS ssi_income_total_moe,
        "B19057_001E"::FLOAT::BIGINT AS public_assistance_income_total,
        "B19057_001M"::FLOAT::BIGINT AS public_assistance_income_total_moe,
        "B19058_001E"::FLOAT::BIGINT AS food_stamps_snap_income_total,
        "B19058_001M"::FLOAT::BIGINT AS food_stamps_snap_income_total_moe,
        "B19101_001E"::FLOAT::BIGINT AS family_income_total,
        "B19101_001M"::FLOAT::BIGINT AS family_income_total_moe,
        "B19113_001E"::FLOAT::BIGINT AS median_family_income_2019,
        "B19113_001M"::FLOAT::BIGINT AS median_family_income_2019_moe,
        "B19127_001E"::FLOAT::BIGINT AS aggregate_family_income_2017,
        "B19127_001M"::FLOAT::BIGINT AS aggregate_family_income_2017_moe,
        "B19201_001E"::FLOAT::BIGINT AS nonfamily_income_total,
        "B19201_001M"::FLOAT::BIGINT AS nonfamily_income_total_moe,
        "B19202_001E"::FLOAT::BIGINT AS median_nonfamily_income_2010,
        "B19202_001M"::FLOAT::BIGINT AS median_nonfamily_income_2010_moe,
        "B19216_001E"::FLOAT::BIGINT AS aggregate_nonfamily_income_2022,
        "B19216_001M"::FLOAT::BIGINT AS aggregate_nonfamily_income_2022_moe,
        "B19301_001E"::FLOAT::BIGINT AS per_capita_income_2014,
        "B19301_001M"::FLOAT::BIGINT AS per_capita_income_2014_moe,
        "B20001_001E"::FLOAT::BIGINT AS earnings_total_2017,
        "B20001_001M"::FLOAT::BIGINT AS earnings_total_2017_moe,
        "B20002_001E"::FLOAT::BIGINT AS median_earnings_2010,
        "B20002_001M"::FLOAT::BIGINT AS median_earnings_2010_moe,
        "B23001_001E"::FLOAT::BIGINT AS employment_status_total,
        "B23001_001M"::FLOAT::BIGINT AS employment_status_total_moe,
        "B23001_002E"::FLOAT::BIGINT AS employment_male,
        "B23001_002M"::FLOAT::BIGINT AS employment_male_moe,
        "B23001_003E"::FLOAT::BIGINT AS employment_male_16_to_19,
        "B23001_003M"::FLOAT::BIGINT AS employment_male_16_to_19_moe,
        "B23001_004E"::FLOAT::BIGINT AS employment_male_16_to_19_in_labor_force,
        "B23001_004M"::FLOAT::BIGINT AS employment_male_16_to_19_in_labor_force_moe,
        "B23001_005E"::FLOAT::BIGINT AS employment_male_16_to_19_in_armed_forces,
        "B23001_005M"::FLOAT::BIGINT AS employment_male_16_to_19_in_armed_forces_moe,
        "B23001_006E"::FLOAT::BIGINT AS employment_male_16_to_19_civilian,
        "B23001_006M"::FLOAT::BIGINT AS employment_male_16_to_19_civilian_moe,
        "B23001_007E"::FLOAT::BIGINT AS employment_male_16_to_19_employed,
        "B23001_007M"::FLOAT::BIGINT AS employment_male_16_to_19_employed_moe,
        "B23025_001E"::FLOAT::BIGINT AS employment_total_16_plus,
        "B23025_001M"::FLOAT::BIGINT AS employment_total_16_plus_moe,
        "B23025_002E"::FLOAT::BIGINT AS employment_in_labor_force,
        "B23025_002M"::FLOAT::BIGINT AS employment_in_labor_force_moe,
        "B23025_003E"::FLOAT::BIGINT AS employment_civilian_labor_force,
        "B23025_003M"::FLOAT::BIGINT AS employment_civilian_labor_force_moe,
        "B23025_004E"::FLOAT::BIGINT AS employment_employed,
        "B23025_004M"::FLOAT::BIGINT AS employment_employed_moe,
        "B23025_005E"::FLOAT::BIGINT AS employment_unemployed,
        "B23025_005M"::FLOAT::BIGINT AS employment_unemployed_moe,
        "B23025_006E"::FLOAT::BIGINT AS employment_armed_forces,
        "B23025_006M"::FLOAT::BIGINT AS employment_armed_forces_moe,
        "B23025_007E"::FLOAT::BIGINT AS employment_not_in_labor_force,
        "B23025_007M"::FLOAT::BIGINT AS employment_not_in_labor_force_moe,
        "B25003_001E"::FLOAT::BIGINT AS tenure_total,
        "B25003_001M"::FLOAT::BIGINT AS tenure_total_moe,
        "B25003_002E"::FLOAT::BIGINT AS tenure_owner_occupied,
        "B25003_002M"::FLOAT::BIGINT AS tenure_owner_occupied_moe,
        "B25003_003E"::FLOAT::BIGINT AS tenure_renter_occupied,
        "B25003_003M"::FLOAT::BIGINT AS tenure_renter_occupied_moe,
        "B25058_001E"::FLOAT::BIGINT AS median_contract_rent,
        "B25058_001M"::FLOAT::BIGINT AS median_contract_rent_moe,
        "B25061_001E"::FLOAT::BIGINT AS rent_asked_total,
        "B25061_001M"::FLOAT::BIGINT AS rent_asked_total_moe,
        "B25064_001E"::FLOAT::BIGINT AS median_gross_rent,
        "B25064_001M"::FLOAT::BIGINT AS median_gross_rent_moe,
        "B27001_001E"::FLOAT::BIGINT AS health_insurance_total,
        "B27001_001M"::FLOAT::BIGINT AS health_insurance_total_moe,
        "B27001_002E"::FLOAT::BIGINT AS health_insurance_male,
        "B27001_002M"::FLOAT::BIGINT AS health_insurance_male_moe,
        "B27001_003E"::FLOAT::BIGINT AS health_insurance_male_under_6,
        "B27001_003M"::FLOAT::BIGINT AS health_insurance_male_under_6_moe,
        "B27001_004E"::FLOAT::BIGINT AS health_insurance_male_under_6_with_insurance,
        "B27001_004M"::FLOAT::BIGINT AS health_insurance_male_under_6_with_insurance_moe,
        "B27001_005E"::FLOAT::BIGINT AS health_insurance_male_under_6_no_insurance,
        "B27001_005M"::FLOAT::BIGINT AS health_insurance_male_under_6_no_insurance_moe,
        "C24010_001E"::FLOAT::BIGINT AS occupation_total,
        "C24010_001M"::FLOAT::BIGINT AS occupation_total_moe,
        "C24010_002E"::FLOAT::BIGINT AS occupation_total_male,
        "C24010_002M"::FLOAT::BIGINT AS occupation_total_male_moe,
        "C24010_003E"::FLOAT::BIGINT AS occupation_male_management_business_science_arts,
        "C24010_003M"::FLOAT::BIGINT AS occupation_male_management_business_science_arts_moe,
        "C24010_004E"::FLOAT::BIGINT AS occupation_male_management_business_financial,
        "C24010_004M"::FLOAT::BIGINT AS occupation_male_management_business_financial_moe,
        "C24010_005E"::FLOAT::BIGINT AS occupation_male_management_financial,
        "C24010_005M"::FLOAT::BIGINT AS occupation_male_management_financial_moe,
        "C24010_006E"::FLOAT::BIGINT AS occupation_male_business_financial_operations,
        "C24010_006M"::FLOAT::BIGINT AS occupation_male_business_financial_operations_moe,
        "C24010_007E"::FLOAT::BIGINT AS occupation_male_computer_engineering_science,
        "C24010_007M"::FLOAT::BIGINT AS occupation_male_computer_engineering_science_moe,
        "C24010_008E"::FLOAT::BIGINT AS occupation_male_computer_mathematical,
        "C24010_008M"::FLOAT::BIGINT AS occupation_male_computer_mathematical_moe,
        "C24010_009E"::FLOAT::BIGINT AS occupation_male_architecture_engineering,
        "C24010_009M"::FLOAT::BIGINT AS occupation_male_architecture_engineering_moe,
        "C24010_010E"::FLOAT::BIGINT AS occupation_male_life_physical_science,
        "C24010_010M"::FLOAT::BIGINT AS occupation_male_life_physical_science_moe,
        "C24010_011E"::FLOAT::BIGINT AS occupation_male_education_legal_media,
        "C24010_011M"::FLOAT::BIGINT AS occupation_male_education_legal_media_moe,
        "C24010_012E"::FLOAT::BIGINT AS occupation_male_community_service,
        "C24010_012M"::FLOAT::BIGINT AS occupation_male_community_service_moe,
        "C24010_013E"::FLOAT::BIGINT AS occupation_male_education_training_library,
        "C24010_013M"::FLOAT::BIGINT AS occupation_male_education_training_library_moe,
        "C24010_014E"::FLOAT::BIGINT AS occupation_male_education_media,
        "C24010_014M"::FLOAT::BIGINT AS occupation_male_education_media_moe,
        "C24030_001E"::FLOAT::BIGINT AS industry_total,
        "C24030_001M"::FLOAT::BIGINT AS industry_total_moe,
        "C24030_002E"::FLOAT::BIGINT AS industry_total_male,
        "C24030_002M"::FLOAT::BIGINT AS industry_total_male_moe,
        "C24030_003E"::FLOAT::BIGINT AS industry_male_agriculture_mining,
        "C24030_003M"::FLOAT::BIGINT AS industry_male_agriculture_mining_moe
    FROM raw.acs5_congressional_district
)

SELECT * 
FROM source