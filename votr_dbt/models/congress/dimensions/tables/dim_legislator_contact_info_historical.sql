{{ config(
    materialized='table'
) }}

select
    bioguide_id,
    address,
    office_phone_number,
    contact_form,
    is_current_member,
    office_address,
    office_city,
    office_zipcode,
    official_website_url
from {{ ref('stg_legislator_contact_info') }}
