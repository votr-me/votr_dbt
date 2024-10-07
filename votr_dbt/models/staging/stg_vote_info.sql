{{ config(materialized='incremental') }}

SELECT
    vote_id,
    type,
    number,
    congress,
    session,
    category,
    title,
    subject,
    purpose,
    question,
    author,
    date::timestamp AS date,
    updated_at::timestamp AS updated_at,
    requires,
    result,
    result_text,
    source_url,
    record_modified
FROM {{ source('raw', 'votes_info') }}
