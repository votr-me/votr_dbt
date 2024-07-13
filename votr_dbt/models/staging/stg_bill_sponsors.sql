SELECT
    "bioguideId" AS bioguide_id,
    "bill_id" AS bill_id,
    CASE
        WHEN "isByRequest" = 'N' THEN FALSE
        WHEN "isByRequest" = 'Y' THEN TRUE
        ELSE NULL
    END AS is_by_request
from {{ source('raw', 'bill_sponsors') }}
