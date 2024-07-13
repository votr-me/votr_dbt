with base as (
    select
        bioguide_id,
        "address" as address,
        "phone" as office_phone_number,
        "contact_form" as contact_form,
        "currentMember"::bool as is_current_member,
        "addressInformation_officeAddress" as office_address,
        "addressInformation_city" as office_city,
        case
            when length(regexp_replace(trim("addressInformation_zipCode"), '[^0-9]', '')) = 5 then
                cast(regexp_replace(trim("addressInformation_zipCode"), '[^0-9]', '') as integer)
            else
                cast(lpad(regexp_replace(trim("addressInformation_zipCode"), '[^0-9]', ''), 5, '0') as integer)
        end as office_zipcode,
        "officialWebsiteUrl" as official_website_url
    from {{ source('raw', 'congress_members') }}
)

select * from base
