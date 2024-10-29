{{ config(materialized='incremental', primary_key='id') }}
SELECT
    {{ dbt_utils.generate_surrogate_key(['bioguideId', 'bill_id']) }} AS id,
    "bioguideId" AS bioguide_id,
    bill_id
FROM {{ source('raw', 'bill_sponsors') }}
