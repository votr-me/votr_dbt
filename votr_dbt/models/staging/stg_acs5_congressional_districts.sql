{{ config(
    materialized='table'
) }}

WITH source AS (
    SELECT
        CAST("year" as INTEGER) as year,
        CAST("state" as VARCHAR) as state_fip,
        CAST("congressional_district" as VARCHAR) as congressional_district,
        CAST("B01001_001E"::FLOAT as BIGINT) AS sex_total,
        CAST("B01001_001M"::FLOAT as BIGINT) AS sex_total_moe,
        CAST("B01001_002E"::FLOAT as BIGINT) AS sex_male,
        CAST("B01001_002M"::FLOAT as BIGINT) AS sex_male_moe,
        CAST("B01001_003E"::FLOAT as BIGINT) AS sex_male_under_5,
        CAST("B01001_003M"::FLOAT as BIGINT) AS sex_male_under_5_moe,
        CAST("B01001_004E"::FLOAT as BIGINT) AS sex_male_5_to_9,
        CAST("B01001_004M"::FLOAT as BIGINT) AS sex_male_5_to_9_moe,
        CAST("B01001_005E"::FLOAT as BIGINT) AS sex_male_10_to_14,
        CAST("B01001_005M"::FLOAT as BIGINT) AS sex_male_10_to_14_moe,
        CAST("B01001_006E"::FLOAT as BIGINT) AS sex_male_15_to_17,
        CAST("B01001_006M"::FLOAT as BIGINT) AS sex_male_15_to_17_moe,
        CAST("B01001_007E"::FLOAT as BIGINT) AS sex_male_18_to_19,
        CAST("B01001_007M"::FLOAT as BIGINT) AS sex_male_18_to_19_moe,
        CAST("B01001_008E"::FLOAT as BIGINT) AS sex_male_20,
        CAST("B01001_008M"::FLOAT as BIGINT) AS sex_male_20_moe,
        CAST("B01001_009E"::FLOAT as BIGINT) AS sex_male_21,
        CAST("B01001_009M"::FLOAT as BIGINT) AS sex_male_21_moe,
        CAST("B01001_010E"::FLOAT as BIGINT) AS sex_male_22_to_24,
        CAST("B01001_010M"::FLOAT as BIGINT) AS sex_male_22_to_24_moe,
        CAST("B01001_011E"::FLOAT as BIGINT) AS sex_male_25_to_29,
        CAST("B01001_011M"::FLOAT as BIGINT) AS sex_male_25_to_29_moe,
        CAST("B01001_012E"::FLOAT as BIGINT) AS sex_male_30_to_34,
        CAST("B01001_012M"::FLOAT as BIGINT) AS sex_male_30_to_34_moe,
        CAST("B01001_013E"::FLOAT as BIGINT) AS sex_male_35_to_39,
        CAST("B01001_013M"::FLOAT as BIGINT) AS sex_male_35_to_39_moe,
        CAST("B01001_014E"::FLOAT as BIGINT) AS sex_male_40_to_44,
        CAST("B01001_014M"::FLOAT as BIGINT) AS sex_male_40_to_44_moe,
        CAST("B01001_015E"::FLOAT as BIGINT) AS sex_male_45_to_49,
        CAST("B01001_015M"::FLOAT as BIGINT) AS sex_male_45_to_49_moe,
        CAST("B01001_016E"::FLOAT as BIGINT) AS sex_male_50_to_54,
        CAST("B01001_016M"::FLOAT as BIGINT) AS sex_male_50_to_54_moe,
        CAST("B01001_017E"::FLOAT as BIGINT) AS sex_male_55_to_59,
        CAST("B01001_017M"::FLOAT as BIGINT) AS sex_male_55_to_59_moe,
        CAST("B01001_018E"::FLOAT as BIGINT) AS sex_male_60_to_61,
        CAST("B01001_018M"::FLOAT as BIGINT) AS sex_male_60_to_61_moe,
        CAST("B01001_019E"::FLOAT as BIGINT) AS sex_male_62_to_64,
        CAST("B01001_019M"::FLOAT as BIGINT) AS sex_male_62_to_64_moe,
        CAST("B01001_020E"::FLOAT as BIGINT) AS sex_male_65_to_66,
        CAST("B01001_020M"::FLOAT as BIGINT) AS sex_male_65_to_66_moe,
        CAST("B01001_021E"::FLOAT as BIGINT) AS sex_male_67_to_69,
        CAST("B01001_021M"::FLOAT as BIGINT) AS sex_male_67_to_69_moe,
        CAST("B01001_022E"::FLOAT as BIGINT) AS sex_male_70_to_74,
        CAST("B01001_022M"::FLOAT as BIGINT) AS sex_male_70_to_74_moe,
        CAST("B01001_023E"::FLOAT as BIGINT) AS sex_male_75_to_79,
        CAST("B01001_023M"::FLOAT as BIGINT) AS sex_male_75_to_79_moe,
        CAST("B01001_024E"::FLOAT as BIGINT) AS sex_male_80_to_84,
        CAST("B01001_024M"::FLOAT as BIGINT) AS sex_male_80_to_84_moe,
        CAST("B01001_025E"::FLOAT as BIGINT) AS sex_male_85_and_over,
        CAST("B01001_025M"::FLOAT as BIGINT) AS sex_male_85_and_over_moe,
        CAST("B01001_026E"::FLOAT as BIGINT) AS sex_female,
        CAST("B01001_026M"::FLOAT as BIGINT) AS sex_female_moe,
        CAST("B01001_027E"::FLOAT as BIGINT) AS sex_female_under_5,
        CAST("B01001_027M"::FLOAT as BIGINT) AS sex_female_under_5_moe,
        CAST("B01001_028E"::FLOAT as BIGINT) AS sex_female_5_to_9,
        CAST("B01001_028M"::FLOAT as BIGINT) AS sex_female_5_to_9_moe,
        CAST("B01001_029E"::FLOAT as BIGINT) AS sex_female_10_to_14,
        CAST("B01001_029M"::FLOAT as BIGINT) AS sex_female_10_to_14_moe,
        CAST("B01001_030E"::FLOAT as BIGINT) AS sex_female_15_to_17,
        CAST("B01001_030M"::FLOAT as BIGINT) AS sex_female_15_to_17_moe,
        CAST("B01001_031E"::FLOAT as BIGINT) AS sex_female_18_to_19,
        CAST("B01001_031M"::FLOAT as BIGINT) AS sex_female_18_to_19_moe,
        CAST("B01001_032E"::FLOAT as BIGINT) AS sex_female_20,
        CAST("B01001_032M"::FLOAT as BIGINT) AS sex_female_20_moe,
        CAST("B01001_033E"::FLOAT as BIGINT) AS sex_female_21,
        CAST("B01001_033M"::FLOAT as BIGINT) AS sex_female_21_moe,
        CAST("B01001_034E"::FLOAT as BIGINT) AS sex_female_22_to_24,
        CAST("B01001_034M"::FLOAT as BIGINT) AS sex_female_22_to_24_moe,
        CAST("B01001_035E"::FLOAT as BIGINT) AS sex_female_25_to_29,
        CAST("B01001_035M"::FLOAT as BIGINT) AS sex_female_25_to_29_moe,
        CAST("B01001_036E"::FLOAT as BIGINT) AS sex_female_30_to_34,
        CAST("B01001_036M"::FLOAT as BIGINT) AS sex_female_30_to_34_moe,
        CAST("B01001_037E"::FLOAT as BIGINT) AS sex_female_35_to_39,
        CAST("B01001_037M"::FLOAT as BIGINT) AS sex_female_35_to_39_moe,
        CAST("B01001_038E"::FLOAT as BIGINT) AS sex_female_40_to_44,
        CAST("B01001_038M"::FLOAT as BIGINT) AS sex_female_40_to_44_moe,
        CAST("B01001_039E"::FLOAT as BIGINT) AS sex_female_45_to_49,
        CAST("B01001_039M"::FLOAT as BIGINT) AS sex_female_45_to_49_moe,
        CAST("B01001_040E"::FLOAT as BIGINT) AS sex_female_50_to_54,
        CAST("B01001_040M"::FLOAT as BIGINT) AS sex_female_50_to_54_moe,
        CAST("B01001_041E"::FLOAT as BIGINT) AS sex_female_55_to_59,
        CAST("B01001_041M"::FLOAT as BIGINT) AS sex_female_55_to_59_moe,
        CAST("B01001_042E"::FLOAT as BIGINT) AS sex_female_60_to_61,
        CAST("B01001_042M"::FLOAT as BIGINT) AS sex_female_60_to_61_moe,
        CAST("B01001_043E"::FLOAT as BIGINT) AS sex_female_62_to_64,
        CAST("B01001_043M"::FLOAT as BIGINT) AS sex_female_62_to_64_moe,
        CAST("B01001_044E"::FLOAT as BIGINT) AS sex_female_65_to_66,
        CAST("B01001_044M"::FLOAT as BIGINT) AS sex_female_65_to_66_moe,
        CAST("B01001_045E"::FLOAT as BIGINT) AS sex_female_67_to_69,
        CAST("B01001_045M"::FLOAT as BIGINT) AS sex_female_67_to_69_moe,
        CAST("B01001_046E"::FLOAT as BIGINT) AS sex_female_70_to_74,
        CAST("B01001_046M"::FLOAT as BIGINT) AS sex_female_70_to_74_moe,
        CAST("B01001_047E"::FLOAT as BIGINT) AS sex_female_75_to_79,
        CAST("B01001_047M"::FLOAT as BIGINT) AS sex_female_75_to_79_moe,
        CAST("B01001_048E"::FLOAT as BIGINT) AS sex_female_80_to_84,
        CAST("B01001_048M"::FLOAT as BIGINT) AS sex_female_80_to_84_moe,
        CAST("B01001_049E"::FLOAT as BIGINT) AS sex_female_85_and_over,
        CAST("B01001_049M"::FLOAT as BIGINT) AS sex_female_85_and_over_moe,
        CAST("B01003_001E"::FLOAT as BIGINT) AS total_population,
        CAST("B01003_001M"::FLOAT as BIGINT) AS total_population_moe,
        CAST("B02001_002E"::FLOAT as BIGINT) AS race_white_total,
        CAST("B02001_002M"::FLOAT as BIGINT) AS race_white_total_moe,
        CAST("B02001_003E"::FLOAT as BIGINT) AS race_black_total,
        CAST("B02001_003M"::FLOAT as BIGINT) AS race_black_total_moe,
        CAST("B02001_004E"::FLOAT as BIGINT) AS race_native_total,
        CAST("B02001_004M"::FLOAT as BIGINT) AS race_native_total_moe,
        CAST("B02001_005E"::FLOAT as BIGINT) AS race_asian_total,
        CAST("B02001_005M"::FLOAT as BIGINT) AS race_asian_total_moe,
        CAST("B02001_006E"::FLOAT as BIGINT) AS race_pacific_islander_total,
        CAST("B02001_006M"::FLOAT as BIGINT) AS race_pacific_islander_total_moe,
        CAST("B02001_007E"::FLOAT as BIGINT) AS race_other_total,
        CAST("B02001_007M"::FLOAT as BIGINT) AS race_other_total_moe,
        CAST("B02001_008E"::FLOAT as BIGINT) AS race_two_or_more_total,
        CAST("B02001_008M"::FLOAT as BIGINT) AS race_two_or_more_total_moe,
        CAST("B03001_003E"::FLOAT as BIGINT) AS hispanic_total,
        CAST("B03001_003M"::FLOAT as BIGINT) AS hispanic_total_moe,
        CAST("B11001_001E"::FLOAT as BIGINT) AS hh_type_total,
        CAST("B11001_001M"::FLOAT as BIGINT) AS hh_type_total_moe,
        CAST("B11001_002E"::FLOAT as BIGINT) AS family_hh_total,
        CAST("B11001_002M"::FLOAT as BIGINT) AS family_hh_total_moe,
        CAST("B11001_003E"::FLOAT as BIGINT) AS family_hh_married,
        CAST("B11001_003M"::FLOAT as BIGINT) AS family_hh_married_moe,
        CAST("B11001_004E"::FLOAT as BIGINT) AS family_hh_other,
        CAST("B11001_004M"::FLOAT as BIGINT) AS family_hh_other_moe,
        CAST("B11001_005E"::FLOAT as BIGINT) AS male_hh_no_spouse,
        CAST("B11001_005M"::FLOAT as BIGINT) AS male_hh_no_spouse_moe,
        CAST("B11001_006E"::FLOAT as BIGINT) AS female_hh_no_spouse,
        CAST("B11001_006M"::FLOAT as BIGINT) AS female_hh_no_spouse_moe,
        CAST("B11001_007E"::FLOAT as BIGINT) AS nonfamily_hh_total,
        CAST("B11001_007M"::FLOAT as BIGINT) AS nonfamily_hh_total_moe,
        CAST("B11001_008E"::FLOAT as BIGINT) AS nonfamily_hh_alone,
        CAST("B11001_008M"::FLOAT as BIGINT) AS nonfamily_hh_alone_moe,
        CAST("B11001_009E"::FLOAT as BIGINT) AS nonfamily_hh_not_alone,
        CAST("B11001_009M"::FLOAT as BIGINT) AS nonfamily_hh_not_alone_moe,
        CAST("B11005_001E"::FLOAT as BIGINT) AS hh_with_children_total,
        CAST("B11005_001M"::FLOAT as BIGINT) AS hh_with_children_total_moe,
        CAST("B11005_002E"::FLOAT as BIGINT) AS hh_with_children,
        CAST("B11005_002M"::FLOAT as BIGINT) AS hh_with_children_moe,
        CAST("B11005_003E"::FLOAT as BIGINT) AS family_hh_with_children,
        CAST("B11005_003M"::FLOAT as BIGINT) AS family_hh_with_children_moe,
        CAST("B11005_004E"::FLOAT as BIGINT) AS married_hh_with_children,
        CAST("B11005_004M"::FLOAT as BIGINT) AS married_hh_with_children_moe,
        CAST("B11005_005E"::FLOAT as BIGINT) AS other_family_hh_with_children,
        CAST("B11005_005M"::FLOAT as BIGINT) AS other_family_hh_with_children_moe,
        CAST("B11005_006E"::FLOAT as BIGINT) AS male_hh_with_children,
        CAST("B11005_006M"::FLOAT as BIGINT) AS male_hh_with_children_moe,
        CAST("B11005_007E"::FLOAT as BIGINT) AS female_hh_with_children,
        CAST("B11005_007M"::FLOAT as BIGINT) AS female_hh_with_children_moe,
        CAST("B11005_008E"::FLOAT as BIGINT) AS nonfamily_hh_with_children,
        CAST("B11005_008M"::FLOAT as BIGINT) AS nonfamily_hh_with_children_moe,
        CAST("B11005_009E"::FLOAT as BIGINT) AS nonfamily_male_with_children,
        CAST("B11005_009M"::FLOAT as BIGINT) AS nonfamily_male_with_children_moe,
        CAST("B11005_010E"::FLOAT as BIGINT) AS nonfamily_female_with_children,
        CAST("B11005_010M"::FLOAT as BIGINT) AS nonfamily_female_with_children_moe,
        CAST("B11005_011E"::FLOAT as BIGINT) AS hh_without_children,
        CAST("B11005_011M"::FLOAT as BIGINT) AS hh_without_children_moe,
        CAST("B11005_012E"::FLOAT as BIGINT) AS family_hh_without_children,
        CAST("B11005_012M"::FLOAT as BIGINT) AS family_hh_without_children_moe,
        CAST("B11005_013E"::FLOAT as BIGINT) AS married_hh_without_children,
        CAST("B11005_013M"::FLOAT as BIGINT) AS married_hh_without_children_moe,
        CAST("B11005_014E"::FLOAT as BIGINT) AS other_family_hh_without_children,
        CAST("B11005_014M"::FLOAT as BIGINT) AS other_family_hh_without_children_moe,
        CAST("B11005_015E"::FLOAT as BIGINT) AS male_hh_no_children,
        CAST("B11005_015M"::FLOAT as BIGINT) AS male_hh_no_children_moe,
        CAST("B11005_016E"::FLOAT as BIGINT) AS female_hh_no_children,
        CAST("B11005_016M"::FLOAT as BIGINT) AS female_hh_no_children_moe,
        CAST("B11005_017E"::FLOAT as BIGINT) AS nonfamily_hh_no_children,
        CAST("B11005_017M"::FLOAT as BIGINT) AS nonfamily_hh_no_children_moe,
        CAST("B11005_018E"::FLOAT as BIGINT) AS nonfamily_male_no_children,
        CAST("B11005_018M"::FLOAT as BIGINT) AS nonfamily_male_no_children_moe,
        CAST("B11005_019E"::FLOAT as BIGINT) AS nonfamily_female_no_children,
        CAST("B11005_019M"::FLOAT as BIGINT) AS nonfamily_female_no_children_moe,
        CAST("B15003_001E"::FLOAT as BIGINT) AS education_attainment_total,
        CAST("B15003_001M"::FLOAT as BIGINT) AS education_attainment_total_moe,
        CAST("B15003_017E"::FLOAT as BIGINT) AS high_school_diploma_total,
        CAST("B15003_017M"::FLOAT as BIGINT) AS high_school_diploma_total_moe,
        CAST("B15003_022E"::FLOAT as BIGINT) AS bachelor_degree_total,
        CAST("B15003_022M"::FLOAT as BIGINT) AS bachelor_degree_total_moe,
        CAST("B15003_023E"::FLOAT as BIGINT) AS master_degree_total,
        CAST("B15003_023M"::FLOAT as BIGINT) AS master_degree_total_moe,
        CAST("B15003_024E"::FLOAT as BIGINT) AS professional_school_degree_total,
        CAST("B15003_024M"::FLOAT as BIGINT) AS professional_school_degree_total_moe,
        CAST("B15003_025E"::FLOAT as BIGINT) AS doctorate_degree_total,
        CAST("B15003_025M"::FLOAT as BIGINT) AS doctorate_degree_total_moe,
        CAST("B16001_001E"::FLOAT as BIGINT) AS language_spoken_at_home_total,
        CAST("B16001_001M"::FLOAT as BIGINT) AS language_spoken_at_home_total_moe,
        CAST("B16001_002E"::FLOAT as BIGINT) AS speak_only_english,
        CAST("B16001_002M"::FLOAT as BIGINT) AS speak_only_english_moe,
        CAST("B16001_003E"::FLOAT as BIGINT) AS speak_spanish,
        CAST("B16001_003M"::FLOAT as BIGINT) AS speak_spanish_moe,
        CAST("B16001_004E"::FLOAT as BIGINT) AS spanish_speak_english_well,
        CAST("B16001_004M"::FLOAT as BIGINT) AS spanish_speak_english_well_moe,
        CAST("B16001_005E"::FLOAT as BIGINT) AS spanish_speak_english_less_than_well,
        CAST("B16001_005M"::FLOAT as BIGINT) AS spanish_speak_english_less_than_well_moe,
        CAST("B16001_006E"::FLOAT as BIGINT) AS speak_french,
        CAST("B16001_006M"::FLOAT as BIGINT) AS speak_french_moe,
        CAST("B17001_001E"::FLOAT as BIGINT) AS poverty_status_total,
        CAST("B17001_001M"::FLOAT as BIGINT) AS poverty_status_total_moe,
        CAST("B17001_002E"::FLOAT as BIGINT) AS income_below_poverty_total,
        CAST("B17001_002M"::FLOAT as BIGINT) AS income_below_poverty_total_moe,
        CAST("B19001_001E"::FLOAT as BIGINT) AS hh_income_total,
        CAST("B19001_001M"::FLOAT as BIGINT) AS hh_income_total_moe,
        CAST("B19001_002E"::FLOAT as BIGINT) AS hh_income_less_10K,
        CAST("B19001_002M"::FLOAT as BIGINT) AS hh_income_less_10k_moe,
        CAST("B19001_003E"::FLOAT as BIGINT) AS hh_income_10k_to_14k,
        CAST("B19001_003M"::FLOAT as BIGINT) AS hh_income_10k_to_14k_moe,
        CAST("B19001_004E"::FLOAT as BIGINT) AS hh_income_15k_to_19k,
        CAST("B19001_004M"::FLOAT as BIGINT) AS hh_income_15k_to_19k_moe,
        CAST("B19001_005E"::FLOAT as BIGINT) AS hh_income_20k_to_24k,
        CAST("B19001_005M"::FLOAT as BIGINT) AS hh_income_20k_to_24k_moe,
        CAST("B19001_006E"::FLOAT as BIGINT) AS hh_income_25k_to_29k,
        CAST("B19001_006M"::FLOAT as BIGINT) AS hh_income_25k_to_29k_moe,
        CAST("B19001_007E"::FLOAT as BIGINT) AS hh_income_30k_to_34k,
        CAST("B19001_007M"::FLOAT as BIGINT) AS hh_income_30k_to_34k_moe,
        CAST("B19001_008E"::FLOAT as BIGINT) AS hh_income_35k_to_39k,
        CAST("B19001_008M"::FLOAT as BIGINT) AS hh_income_35k_to_39k_moe,
        CAST("B19001_009E"::FLOAT as BIGINT) AS hh_income_40k_to_44k,
        CAST("B19001_009M"::FLOAT as BIGINT) AS hh_income_40k_to_44k_moe,
        CAST("B19001_010E"::FLOAT as BIGINT) AS hh_income_45k_to_49k,
        CAST("B19001_010M"::FLOAT as BIGINT) AS hh_income_45k_to_49k_moe,
        CAST("B19001_011E"::FLOAT as BIGINT) AS hh_income_50k_to_59k,
        CAST("B19001_011M"::FLOAT as BIGINT) AS hh_income_50k_to_59k_moe,
        CAST("B19001_012E"::FLOAT as BIGINT) AS hh_income_60k_to_74k,
        CAST("B19001_012M"::FLOAT as BIGINT) AS hh_income_60k_to_74k_moe,
        CAST("B19001_013E"::FLOAT as BIGINT) AS hh_income_75k_to_99k,
        CAST("B19001_013M"::FLOAT as BIGINT) AS hh_income_75k_to_99k_moe,
        CAST("B19001_014E"::FLOAT as BIGINT) AS hh_income_100k_to_124k,
        CAST("B19001_014M"::FLOAT as BIGINT) AS hh_income_100k_to_124k_moe,
        CAST("B19001_015E"::FLOAT as BIGINT) AS hh_income_125k_to_149k,
        CAST("B19001_015M"::FLOAT as BIGINT) AS hh_income_125k_to_149k_moe,
        CAST("B19001_016E"::FLOAT as BIGINT) AS hh_income_150k_to_199k,
        CAST("B19001_016M"::FLOAT as BIGINT) AS hh_income_150k_to_199k_moe,
        CAST("B19001_017E"::FLOAT as BIGINT) AS hh_income_200k_or_more,
        CAST("B19001_017M"::FLOAT as BIGINT) AS hh_income_200k_or_more_moe,
        CAST("B19013_001E"::FLOAT as BIGINT) AS median_hh_income,
        CAST("B19013_001M"::FLOAT as BIGINT) AS median_hh_income_moe,
        CAST("B19025_001E"::FLOAT as BIGINT) AS aggregate_hh_income,
        CAST("B19025_001M"::FLOAT as BIGINT) AS aggregate_hh_income_moe,
        CAST("B19101_001E"::FLOAT as BIGINT) AS family_income_total,
        CAST("B19101_001M"::FLOAT as BIGINT) AS family_income_total_moe,
        CAST("B19113_001E"::FLOAT as BIGINT) AS median_family_income,
        CAST("B19113_001M"::FLOAT as BIGINT) AS median_family_income_moe,
        CAST("B19127_001E"::FLOAT as BIGINT) AS aggregate_family_income,
        CAST("B19127_001M"::FLOAT as BIGINT) AS aggregate_family_income_moe,
        CAST("B19201_001E"::FLOAT as BIGINT) AS nonfamily_hh_income_total,
        CAST("B19201_001M"::FLOAT as BIGINT) AS nonfamily_hh_income_total_moe,
        CAST("B19202_001E"::FLOAT as BIGINT) AS median_nonfamily_hh_income,
        CAST("B19202_001M"::FLOAT as BIGINT) AS median_nonfamily_hh_income_moe,
        CAST("B19216_001E"::FLOAT as BIGINT) AS nonfamily_income_by_sex,
        CAST("B19216_001M"::FLOAT as BIGINT) AS nonfamily_income_by_sex_moe,
        CAST("B19301_001E"::FLOAT as BIGINT) AS per_capita_income,
        CAST("B19301_001M"::FLOAT as BIGINT) AS per_capita_income_moe,
        CAST("B20001_001E"::FLOAT as BIGINT) AS earnings_total,
        CAST("B20001_001M"::FLOAT as BIGINT) AS earnings_total_moe,
        CAST("B20002_001E"::FLOAT as BIGINT) AS median_earnings,
        CAST("B20002_001M"::FLOAT as BIGINT) AS median_earnings_moe,
        CAST("B23001_001E"::FLOAT as BIGINT) AS employment_status_total,
        CAST("B23001_001M"::FLOAT as BIGINT) AS employment_status_total_moe,
        CAST("B23001_002E"::FLOAT as BIGINT) AS employment_status_male,
        CAST("B23001_002M"::FLOAT as BIGINT) AS employment_status_male_moe,
        CAST("B23025_002E"::FLOAT as BIGINT) AS in_labor_force,
        CAST("B23025_002M"::FLOAT as BIGINT) AS in_labor_force_moe,
        CAST("B23025_003E"::FLOAT as BIGINT) AS civilian_labor_force,
        CAST("B23025_003M"::FLOAT as BIGINT) AS civilian_labor_force_moe,
        CAST("B23025_004E"::FLOAT as BIGINT) AS civilian_employed,
        CAST("B23025_004M"::FLOAT as BIGINT) AS civilian_employed_moe,
        CAST("B23025_005E"::FLOAT as BIGINT) AS civilian_unemployed,
        CAST("B23025_005M"::FLOAT as BIGINT) AS civilian_unemployed_moe,
        CAST("B23025_006E"::FLOAT as BIGINT) AS armed_forces,
        CAST("B23025_006M"::FLOAT as BIGINT) AS armed_forces_moe,
        CAST("B23025_007E"::FLOAT as BIGINT) AS not_in_labor_force,
        CAST("B23025_007M"::FLOAT as BIGINT) AS not_in_labor_force_moe,
        CAST("B25003_001E"::FLOAT as BIGINT) AS tenure_total,
        CAST("B25003_001M"::FLOAT as BIGINT) AS tenure_total_moe,
        CAST("B25003_002E"::FLOAT as BIGINT) AS owner_occupied,
        CAST("B25003_002M"::FLOAT as BIGINT) AS owner_occupied_moe,
        CAST("B25003_003E"::FLOAT as BIGINT) AS renter_occupied,
        CAST("B25003_003M"::FLOAT as BIGINT) AS renter_occupied_moe,
        CAST("B25058_001E"::FLOAT as BIGINT) AS median_contract_rent,
        CAST("B25058_001M"::FLOAT as BIGINT) AS median_contract_rent_moe,
        CAST("B25061_001E"::FLOAT as BIGINT) AS rent_asked_total,
        CAST("B25061_001M"::FLOAT as BIGINT) AS rent_asked_total_moe,
        CAST("B25064_001E"::FLOAT as BIGINT) AS median_gross_rent,
        CAST("B25064_001M"::FLOAT as BIGINT) AS median_gross_rent_moe,
        CAST("B27001_001E"::FLOAT as BIGINT) AS health_insurance_total,
        CAST("B27001_001M"::FLOAT as BIGINT) AS health_insurance_total_moe,
        CAST("B27001_002E"::FLOAT as BIGINT) AS health_insurance_male,
        CAST("B27001_002M"::FLOAT as BIGINT) AS health_insurance_male_moe,
        CAST("B27001_030E"::FLOAT as BIGINT) AS health_insurance_female,
        CAST("B27001_030M"::FLOAT as BIGINT) AS health_insurance_female_moe,
        CAST("C24010_001E"::FLOAT as BIGINT) AS occupation_total,
        CAST("C24010_001M"::FLOAT as BIGINT) AS occupation_total_moe,
        CAST("C24010_002E"::FLOAT as BIGINT) AS occupation_male_total,
        CAST("C24010_002M"::FLOAT as BIGINT) AS occupation_male_total_moe,
        CAST("C24010_003E"::FLOAT as BIGINT) AS male_management_arts,
        CAST("C24010_003M"::FLOAT as BIGINT) AS male_management_arts_moe,
        CAST("C24010_004E"::FLOAT as BIGINT) AS male_management_business,
        CAST("C24010_004M"::FLOAT as BIGINT) AS male_management_business_moe,
        CAST("C24010_005E"::FLOAT as BIGINT) AS male_management,
        CAST("C24010_005M"::FLOAT as BIGINT) AS male_management_moe,
        CAST("C24010_006E"::FLOAT as BIGINT) AS male_business_operations,
        CAST("C24010_006M"::FLOAT as BIGINT) AS male_business_operations_moe,
        CAST("C24010_007E"::FLOAT as BIGINT) AS male_computer_science,
        CAST("C24010_007M"::FLOAT as BIGINT) AS male_computer_science_moe,
        CAST("C24010_008E"::FLOAT as BIGINT) AS male_computer_math,
        CAST("C24010_008M"::FLOAT as BIGINT) AS male_computer_math_moe,
        CAST("C24010_009E"::FLOAT as BIGINT) AS male_architecture_engineering,
        CAST("C24010_009M"::FLOAT as BIGINT) AS male_architecture_engineering_moe,
        CAST("C24010_010E"::FLOAT as BIGINT) AS male_life_sciences,
        CAST("C24010_010M"::FLOAT as BIGINT) AS male_life_sciences_moe,
        CAST("C24010_011E"::FLOAT as BIGINT) AS male_education_media,
        CAST("C24010_011M"::FLOAT as BIGINT) AS male_education_media_moe,
        CAST("C24010_012E"::FLOAT as BIGINT) AS male_community_service,
        CAST("C24010_012M"::FLOAT as BIGINT) AS male_community_service_moe,
        CAST("C24010_013E"::FLOAT as BIGINT) AS male_legal,
        CAST("C24010_013M"::FLOAT as BIGINT) AS male_legal_moe,
        CAST("C24010_014E"::FLOAT as BIGINT) AS male_education_library,
        CAST("C24010_014M"::FLOAT as BIGINT) AS male_education_library_moe,
        CAST("C24010_015E"::FLOAT as BIGINT) AS male_arts_media,
        CAST("C24010_015M"::FLOAT as BIGINT) AS male_arts_media_moe,
        CAST("C24010_016E"::FLOAT as BIGINT) AS male_healthcare,
        CAST("C24010_016M"::FLOAT as BIGINT) AS male_healthcare_moe,
        CAST("C24010_017E"::FLOAT as BIGINT) AS male_healthcare_practitioners,
        CAST("C24010_017M"::FLOAT as BIGINT) AS male_healthcare_practitioners_moe,
        CAST("C24010_018E"::FLOAT as BIGINT) AS male_healthcare_technicians,
        CAST("C24010_018M"::FLOAT as BIGINT) AS male_healthcare_technicians_moe,
        CAST("C24010_019E"::FLOAT as BIGINT) AS male_service_occupations,
        CAST("C24010_019M"::FLOAT as BIGINT) AS male_service_occupations_moe,
        CAST("C24010_020E"::FLOAT as BIGINT) AS male_healthcare_support,
        CAST("C24010_020M"::FLOAT as BIGINT) AS male_healthcare_support_moe,
        CAST("C24010_021E"::FLOAT as BIGINT) AS male_protective_service,
        CAST("C24010_021M"::FLOAT as BIGINT) AS male_protective_service_moe,
        CAST("C24010_022E"::FLOAT as BIGINT) AS male_firefighters,
        CAST("C24010_022M"::FLOAT as BIGINT) AS male_firefighters_moe,
        CAST("C24010_023E"::FLOAT as BIGINT) AS male_law_enforcement,
        CAST("C24010_023M"::FLOAT as BIGINT) AS male_law_enforcement_moe,
        CAST("C24010_024E"::FLOAT as BIGINT) AS male_food_service,
        CAST("C24010_024M"::FLOAT as BIGINT) AS male_food_service_moe,
        CAST("C24010_025E"::FLOAT as BIGINT) AS male_cleaning_maintenance,
        CAST("C24010_025M"::FLOAT as BIGINT) AS male_cleaning_maintenance_moe,
        CAST("C24010_026E"::FLOAT as BIGINT) AS male_personal_service,
        CAST("C24010_026M"::FLOAT as BIGINT) AS male_personal_service_moe,
        CAST("C24010_027E"::FLOAT as BIGINT) AS male_sales_office,
        CAST("C24010_027M"::FLOAT as BIGINT) AS male_sales_office_moe,
        CAST("C24030_001E"::FLOAT as BIGINT) AS industry_total,
        CAST("C24030_001M"::FLOAT as BIGINT) AS industry_total_moe,
        CAST("C24030_002E"::FLOAT as BIGINT) AS industry_male_total,
        CAST("C24030_002M"::FLOAT as BIGINT) AS industry_male_total_moe,
        CAST("C24030_003E"::FLOAT as BIGINT) AS male_agriculture,
        CAST("C24030_003M"::FLOAT as BIGINT) AS male_agriculture_moe,
        CAST("C24030_030E"::FLOAT as BIGINT) AS female_agriculture,
        CAST("C24030_030M"::FLOAT as BIGINT) AS female_agriculture_moe,
        CAST("C24030_031E"::FLOAT as BIGINT) AS female_agriculture_forestry_fishing,
        CAST("C24030_031M"::FLOAT as BIGINT) AS female_agriculture_forestry_fishing_moe,
        CAST("C24030_032E"::FLOAT as BIGINT) AS female_mining,
        CAST("C24030_032M"::FLOAT as BIGINT) AS female_mining_moe,
        CAST("C24030_033E"::FLOAT as BIGINT) AS female_construction,
        CAST("C24030_033M"::FLOAT as BIGINT) AS female_construction_moe,
        CAST("C24030_034E"::FLOAT as BIGINT) AS female_manufacturing,
        CAST("C24030_034M"::FLOAT as BIGINT) AS female_manufacturing_moe,
        CAST("C24030_035E"::FLOAT as BIGINT) AS female_wholesale_trade,
        CAST("C24030_035M"::FLOAT as BIGINT) AS female_wholesale_trade_moe,
        CAST("C24030_036E"::FLOAT as BIGINT) AS female_retail_trade,
        CAST("C24030_036M"::FLOAT as BIGINT) AS female_retail_trade_moe,
        CAST("C24030_037E"::FLOAT as BIGINT) AS female_transportation,
        CAST("C24030_037M"::FLOAT as BIGINT) AS female_transportation_moe,
        CAST("C24030_038E"::FLOAT as BIGINT) AS female_utilities,
        CAST("C24030_038M"::FLOAT as BIGINT) AS female_utilities_moe,
        CAST("C24030_039E"::FLOAT as BIGINT) AS female_information,
        CAST("C24030_039M"::FLOAT as BIGINT) AS female_information_moe,
        CAST("C24030_040E"::FLOAT as BIGINT) AS female_finance_insurance,
        CAST("C24030_040M"::FLOAT as BIGINT) AS female_finance_insurance_moe,
        CAST("C24030_041E"::FLOAT as BIGINT) AS female_real_estate,
        CAST("C24030_041M"::FLOAT as BIGINT) AS female_real_estate_moe,
        CAST("C24030_042E"::FLOAT as BIGINT) AS female_professional_services,
        CAST("C24030_042M"::FLOAT as BIGINT) AS female_professional_services_moe,
        CAST("C24030_043E"::FLOAT as BIGINT) AS female_management,
        CAST("C24030_043M"::FLOAT as BIGINT) AS female_management_moe,
        CAST("C24030_044E"::FLOAT as BIGINT) AS female_administrative_support,
        CAST("C24030_044M"::FLOAT as BIGINT) AS female_administrative_support_moe,
        CAST("C24030_045E"::FLOAT as BIGINT) AS female_education,
        CAST("C24030_045M"::FLOAT as BIGINT) AS female_education_moe,
        CAST("C24030_046E"::FLOAT as BIGINT) AS female_healthcare_social_assistance,
        CAST("C24030_046M"::FLOAT as BIGINT) AS female_healthcare_social_assistance_moe,
        CAST("C24030_047E"::FLOAT as BIGINT) AS female_arts_entertainment,
        CAST("C24030_047M"::FLOAT as BIGINT) AS female_arts_entertainment_moe,
        CAST("C24030_048E"::FLOAT as BIGINT) AS female_accommodation_food,
        CAST("C24030_048M"::FLOAT as BIGINT) AS female_accommodation_food_moe,
        CAST("C24030_049E"::FLOAT as BIGINT) AS female_other_services,
        CAST("C24030_049M"::FLOAT as BIGINT) AS female_other_services_moe,
        CAST("C24030_050E"::FLOAT as BIGINT) AS female_public_administration,
        CAST("C24030_050M"::FLOAT as BIGINT) AS female_public_administration_moe
    FROM {{ source('raw', 'acs5_congressional_district')}}
)

SELECT * 
FROM source