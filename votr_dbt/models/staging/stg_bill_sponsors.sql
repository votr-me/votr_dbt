{{ config(materialized='incremental') }}
SELECT
    "bioguideId" AS bioguide_id,
    bill_id
FROM {{ source('raw', 'bill_sponsors') }}
