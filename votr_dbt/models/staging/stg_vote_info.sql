{{ config(materialized='incremental', primary_key='id') }}

SELECT
    {{ dbt_utils.generate_surrogate_key(['vote_id', 'type', 'congress', 'session']) }} AS id,
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
    CAST(date AS TIMESTAMP) AS date,
    CAST(updated_at AS TIMESTAMP) AS updated_at,
    requires,
    result,
    result_text,
    source_url,
    record_modified
FROM {{ source('raw', 'votes_info') }}
