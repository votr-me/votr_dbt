{{ config(
    materialized='view'
) }}

with cd_employment as (select year,
                              state_fip,
                              congressional_district,
                              'us'                                                  as country,
                              employment_status_total                               as cd_employment_status_total,
                              in_labor_force                                        as cd_in_labor_force,
                              in_labor_force::float / employment_status_total       as cd_pct_in_labor_force,
                              not_in_labor_force                                    as cd_not_in_labor_force,
                              not_in_labor_force::float / employment_status_total   as cd_pct_not_in_labor_force,
                              civilian_labor_force                                  as cd_civilian_labor_force,
                              civilian_labor_force::float / employment_status_total as cd_pct_civilian_labor_force,
                              civilian_employed                                     as cd_civilian_employed,
                              civilian_employed::float / employment_status_total    as cd_pct_civilian_employed
                       from public.acs5_cd_jobs_historical
                       where congressional_district != 'ZZ'),

     us_employment as (select year,
                              employment_status_total                               as us_employment_status_total,
                              'us'                                                  as country,
                              in_labor_force                                        as us_in_labor_force,
                              in_labor_force::float / employment_status_total       as us_pct_in_labor_force,
                              not_in_labor_force                                    as us_not_in_labor_force,
                              not_in_labor_force::float / employment_status_total   as us_pct_not_in_labor_force,
                              civilian_labor_force                                  as us_civilian_labor_force,
                              civilian_labor_force::float / employment_status_total as us_pct_civilian_labor_force,
                              civilian_employed                                     as us_civilian_employed,
                              civilian_employed::float / employment_status_total    as us_pct_civilian_employed
                       from public.acs5_us_jobs_historical),

     state_employment as (select year,
                                 state_fip,
                                 'us'                                                  as country,
                                 employment_status_total                               as state_employment_status_total,
                                 in_labor_force                                        as state_in_labor_force,
                                 in_labor_force::float / employment_status_total       as state_pct_in_labor_force,
                                 not_in_labor_force                                    as state_not_in_labor_force,
                                 not_in_labor_force::float / employment_status_total   as state_pct_not_in_labor_force,
                                 civilian_labor_force                                  as state_civilian_labor_force,
                                 civilian_labor_force::float / employment_status_total as state_pct_civilian_labor_force,
                                 civilian_employed                                     as state_civilian_employed,
                                 civilian_employed::float / employment_status_total    as state_pct_civilian_employed
                          from public.acs5_state_jobs_historical)

select
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
from cd_employment
left join state_employment
    on cd_employment.state_fip = state_employment.state_fip
    and cd_employment.year = state_employment.year
left join us_employment
    on cd_employment.country = us_employment.country
    and cd_employment.year = us_employment.year
order by cd_employment.state_fip, cd_employment.congressional_district, cd_employment.year