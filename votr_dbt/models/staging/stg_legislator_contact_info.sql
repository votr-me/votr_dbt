{{ config(materialized='table') }}

with base as (
    SELECT
        bioguide_id,
        address,
        phone AS office_phone_number,
        contact_form,
        CAST(`currentMember` AS BOOL) AS is_current_member,
        `addressInformation_officeAddress` AS office_address,
        `addressInformation_city` AS office_city,
        `officialWebsiteUrl` AS official_website_url,
        CAST(
        CASE
            WHEN LENGTH(REGEXP_REPLACE(TRIM(CAST(`addressInformation_zipCode` AS STRING)), r'[^0-9]', '')) = 5 THEN REGEXP_REPLACE(TRIM(CAST(`addressInformation_zipCode` AS STRING)), r'[^0-9]', '')
            ELSE LPAD(REGEXP_REPLACE(TRIM(CAST(`addressInformation_zipCode` AS STRING)), r'[^0-9]', ''), 5, '0')
        END AS INT64
        ) AS office_zipcode
    FROM
        {{ source('raw', 'legislators') }}
)

select * 
from base
