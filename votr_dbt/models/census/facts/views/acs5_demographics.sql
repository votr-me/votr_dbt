{{ config(
    materialized='view'
) }}


with cd_demographics as (
    select
        year,
        congressional_district,
        state_fip,
        'us' as country,
        sex_total as cd_sex_total,
        sex_male::float / sex_total::float as cd_pct_male,
        sex_male as cd_sex_male,
        sex_female::float / sex_total::float as cd_pct_female,
        (male_voting_age_population::float + female_voting_age_population::float) as cd_total_voting_age,
        (male_voting_age_population::float + female_voting_age_population::float) / sex_total as cd_pct_population_of_voting_age,
        race_white_total as cd_race_white_total,
        race_white_total::float / total_population::float as cd_pct_white,
        race_asian_total as cd_asian_total,
        race_asian_total::float / total_population::float as cd_pct_asian,
        race_black_total as cd_race_black_total,
        race_black_total::float / total_population::float as cd_pct_black,
        hispanic_total as cd_race_hispanic_total,
        hispanic_total::float / total_population::float as cd_pct_hispanic,
        race_pacific_islander_total as cd_race_pacific_islander_total,
        race_pacific_islander_total::float / total_population::float as cd_pct_pacific_islander,
        race_other_total as cd_race_other_total,
        race_other_total::float / total_population as cd_pct_other_race,
        race_two_or_more_total as cd_race_two_or_more_total,
        race_two_or_more_total::float / total_population as cd_pct_two_or_more
    from {{ ref('acs5_cd_demographics_historical')}}
    where congressional_district not like 'ZZ'
    order by state_fip, congressional_district, year
),

state_demographics as (
    select
        year,
        state_fip,
        'us' as country,
        sex_total as state_sex_total,
        sex_male::float / sex_total::float as state_pct_male,
        sex_male as state_sex_male,
        sex_female::float / sex_total::float as state_pct_female,
        (male_voting_age_population::float + female_voting_age_population::float) as state_total_voting_age,
        (male_voting_age_population::float + female_voting_age_population::float) / sex_total as state_pct_population_of_voting_age,
        race_white_total as state_race_white_total,
        race_white_total::float / total_population::float as state_pct_white,
        race_asian_total as state_asian_total,
        race_asian_total::float / total_population::float as state_pct_asian,
        race_black_total as state_race_black_total,
        race_black_total::float / total_population::float as state_pct_black,
        hispanic_total as state_race_hispanic_total,
        hispanic_total::float / total_population::float as state_pct_hispanic,
        race_pacific_islander_total as state_race_pacific_islander_total,
        race_pacific_islander_total::float / total_population::float as state_pct_pacific_islander,
        race_other_total as state_race_other_total,
        race_other_total::float / total_population as state_pct_other_race,
        race_two_or_more_total as state_race_two_or_more_total,
        race_two_or_more_total::float / total_population as state_pct_two_or_more
    from{{ ref('acs5_state_demographics_historical')}}
    order by state_fip, year
),

us_demographics as (
    select
        year,
        'us' as country,
        sex_total as us_sex_total,
        sex_male::float / sex_total::float as us_pct_male,
        sex_male as us_sex_male,
        sex_female::float / sex_total::float as us_pct_female,
        (male_voting_age_population::float + female_voting_age_population::float) as us_total_voting_age,
        (male_voting_age_population::float + female_voting_age_population::float) / sex_total as us_pct_population_of_voting_age,
        race_white_total as us_race_white_total,
        race_white_total::float / total_population::float as us_pct_white,
        race_asian_total as us_asian_total,
        race_asian_total::float / total_population::float as us_pct_asian,
        race_black_total as us_race_black_total,
        race_black_total::float / total_population::float as us_pct_black,
        hispanic_total as us_race_hispanic_total,
        hispanic_total::float / total_population::float as us_pct_hispanic,
        race_pacific_islander_total as us_race_pacific_islander_total,
        race_pacific_islander_total::float / total_population::float as us_pct_pacific_islander,
        race_other_total as us_race_other_total,
        race_other_total::float / total_population as us_pct_other_race,
        race_two_or_more_total as us_race_two_or_more_total,
        race_two_or_more_total::float / total_population as us_pct_two_or_more
    from {{ ref('acs5_us_demographics_historical')}}
    order by year
)

select
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
from cd_demographics
left join state_demographics
    on cd_demographics.state_fip = state_demographics.state_fip
        and cd_demographics.year = state_demographics.year
left join us_demographics
    on cd_demographics.country = us_demographics.country
        and cd_demographics.year = us_demographics.year
order by cd_demographics.state_fip, cd_demographics.congressional_district, cd_demographics.year
