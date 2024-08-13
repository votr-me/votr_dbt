{{ config(
    materialized='table',
    primary_key = 'id'
) }}

SELECT
    {{ dbt_utils.generate_surrogate_key(['year', 'state_fip', 'congressional_district']) }} as id,
    year,
    state_fip,
    congressional_district,
    hh_income_total,
    hh_income_total_moe,
    hh_income_less_10k,
    hh_income_less_10k_moe,
    CASE 
        WHEN hh_income_total = 0 THEN Null
        ELSE hh_income_less_10k::float / hh_income_total::float 
    END as pct_hh_income_less_10k,
    hh_income_10k_to_14k,
    hh_income_10k_to_14k_moe,
    CASE 
        WHEN hh_income_total = 0 THEN Null
        ELSE hh_income_10k_to_14k::float / hh_income_total::float 
    END as pct_hh_income_10k_to_14k,
    hh_income_15k_to_19k,
    hh_income_15k_to_19k_moe,
    CASE 
        WHEN hh_income_total = 0 THEN Null
        ELSE hh_income_15k_to_19k::float / hh_income_total::float 
    END as pct_hh_income_15k_to_19k,
    hh_income_20k_to_24k,
    hh_income_20k_to_24k_moe,
    CASE 
        WHEN hh_income_total = 0 THEN Null
        ELSE hh_income_20k_to_24k::float / hh_income_total::float 
    END as pct_hh_income_20k_to_24k,
    hh_income_25k_to_29k,
    hh_income_25k_to_29k_moe,
    CASE 
        WHEN hh_income_total = 0 THEN Null
        ELSE hh_income_25k_to_29k::float / hh_income_total::float 
    END as pct_hh_income_25k_to_29k,
    hh_income_30k_to_34k,
    hh_income_30k_to_34k_moe,
    CASE 
        WHEN hh_income_total = 0 THEN Null
        ELSE hh_income_30k_to_34k::float / hh_income_total::float 
    END as pct_hh_income_30k_to_34k,
    hh_income_35k_to_39k,
    hh_income_35k_to_39k_moe,
    CASE 
        WHEN hh_income_total = 0 THEN Null
        ELSE hh_income_35k_to_39k::float / hh_income_total::float 
    END as pct_hh_income_35k_to_39k,
    hh_income_40k_to_44k,
    hh_income_40k_to_44k_moe,
    CASE 
        WHEN hh_income_total = 0 THEN Null
        ELSE hh_income_40k_to_44k::float / hh_income_total::float 
    END as pct_hh_income_40k_to_44k,
    hh_income_45k_to_49k,
    hh_income_45k_to_49k_moe,
    CASE 
        WHEN hh_income_total = 0 THEN Null
        ELSE hh_income_45k_to_49k::float / hh_income_total::float 
    END as pct_hh_income_45k_to_49k,   
    hh_income_50k_to_59k,
    hh_income_50k_to_59k_moe,
    CASE 
        WHEN hh_income_total = 0 THEN Null
        ELSE hh_income_50k_to_59k::float / hh_income_total::float 
    END as pct_hh_income_50k_to_59k,
    hh_income_60k_to_74k,
    hh_income_60k_to_74k_moe,
    CASE 
        WHEN hh_income_total = 0 THEN Null
        ELSE hh_income_60k_to_74k::float / hh_income_total::float 
    END as pct_hh_income_60k_to_74k,
    hh_income_75k_to_99k,
    hh_income_75k_to_99k_moe,
    CASE 
        WHEN hh_income_total = 0 THEN Null
        ELSE hh_income_75k_to_99k::float / hh_income_total::float 
    END as pct_hh_income_75k_to_99k,
    hh_income_100k_to_124k,
    hh_income_100k_to_124k_moe,
    CASE 
        WHEN hh_income_total = 0 THEN Null
        ELSE hh_income_100k_to_124k::float / hh_income_total::float 
    END as pct_hh_income_100k_to_124k,
    hh_income_125k_to_149k,
    hh_income_125k_to_149k_moe,
    CASE 
        WHEN hh_income_total = 0 THEN Null
        ELSE hh_income_125k_to_149k::float / hh_income_total::float 
    END as pct_hh_income_125k_to_149k,
    hh_income_150k_to_199k,
    hh_income_150k_to_199k_moe,
    CASE 
        WHEN hh_income_total = 0 THEN Null
        ELSE hh_income_150k_to_199k::float / hh_income_total::float 
    END as pct_hh_income_150k_to_199k,
    hh_income_200k_or_more,
    hh_income_200k_or_more_moe,
    CASE 
        WHEN hh_income_total = 0 THEN Null
        ELSE hh_income_200k_or_more::float / hh_income_total::float 
    END as pct_hh_income_200k_or_more,   
    family_income_total,
    family_income_total_moe,
    median_family_income,
    median_family_income_moe
FROM {{ ref('stg_acs5_congressional_districts') }}
