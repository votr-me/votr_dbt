{{ config(materialized='table') }}

with base as (
    select
        bioguide_id,
        address,
        phone as office_phone_number,
        contact_form,
        "currentMember"::bool as is_current_member,
        "addressInformation_officeAddress" as office_address,
        "addressInformation_city" as office_city,
        "officialWebsiteUrl" as official_website_url,
        case
            when
                length(
                    regexp_replace(
                        trim("addressInformation_zipCode"), '[^0-9]', ''
                    )
                )
                = 5
                then
                    (regexp_replace(
                        trim("addressInformation_zipCode"), '[^0-9]', ''
                    ))::integer
            else
                (lpad(
                    regexp_replace(
                        trim("addressInformation_zipCode"), '[^0-9]', ''
                    ),
                    5,
                    '0'
                ))::integer
        end as office_zipcode
    from {{ source('raw', 'legislators') }}
)

select * from base
