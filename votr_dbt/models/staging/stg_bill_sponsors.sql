SELECT
    bioguide_id,
    bill_id,
    is_by_request
from {{ source('raw', 'bill_sponsors_2') }}
