{{ config(
    materialized='table',
    primary_key = 'id'
) }}

WITH bill_sponsors AS (
    SELECT *
    FROM {{ ref('dim_bill_sponsorships_historical') }}
),

bill_info AS (
    SELECT *
    FROM {{ ref('dim_bill_info_historical') }}
),

total_bills AS (
    SELECT
        bill_sponsors.bioguide_id,
        bill_info.congress,
        bill_sponsors.sponsorship_type,
        COUNT(DISTINCT bill_info.bill_id) AS num_total_bills
    FROM
        bill_sponsors
    LEFT JOIN
        bill_info ON bill_sponsors.bill_id = bill_info.bill_id
    WHERE bill_sponsors.bioguide_id IS NOT NULL
      AND bill_info.congress IS NOT NULL
    GROUP BY 1, 2, 3
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['bill_sponsors.bioguide_id', 'bill_info.policy_area_name', 'bill_sponsors.sponsorship_type', 'bill_info.congress']) }} AS id,
    bill_sponsors.bioguide_id,
    bill_info.policy_area_name,
    bill_sponsors.sponsorship_type,
    bill_info.congress,
    total_bills.num_total_bills,  -- Include in GROUP BY
    COUNT(DISTINCT bill_info.bill_id) AS num_bills_by_policy_area,
    CAST(COUNT(DISTINCT bill_info.bill_id) AS BIGNUMERIC) / CAST(total_bills.num_total_bills AS BIGNUMERIC) AS pct_of_total_bills
FROM
    bill_sponsors
LEFT JOIN
    bill_info ON bill_sponsors.bill_id = bill_info.bill_id
LEFT JOIN
    total_bills ON bill_sponsors.bioguide_id = total_bills.bioguide_id
    AND bill_info.congress = total_bills.congress
    AND bill_sponsors.sponsorship_type = total_bills.sponsorship_type
WHERE bill_sponsors.bioguide_id IS NOT NULL
  AND bill_info.congress IS NOT NULL
  AND bill_info.policy_area_name IS NOT NULL
GROUP BY 1, 2, 3, 4, 5, 6