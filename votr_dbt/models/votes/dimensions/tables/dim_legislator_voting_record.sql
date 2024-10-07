{{ config(
    materialized='incremental'
) }}


with voting_record as (
    select
        bioguide_id,
        vote_id,
        vote_type,
        vote_num,
        vote_session,
        congress,
        legislator_vote
    from {{ ref('stg_legislator_voting_record') }}
),

vote_info as (
    select
        vote_id,
        category,
        title,
        subject,
        purpose,
        question,
        author,
        date,
        updated_at,
        requires,
        result,
        result_text,
        source_url,
        record_modified
    from {{ ref('stg_vote_info') }}
)

select
    voting_record.bioguide_id,
    voting_record.vote_id,
    voting_record.vote_type,
    voting_record.vote_num,
    voting_record.vote_session,
    voting_record.congress,
    voting_record.legislator_vote,
    vote_info.category,
    vote_info.title,
    vote_info.subject,
    vote_info.purpose,
    vote_info.question,
    vote_info.author,
    vote_info.date,
    vote_info.updated_at,
    vote_info.requires,
    vote_info.result,
    vote_info.result_text,
    vote_info.source_url,
    vote_info.record_modified
from voting_record left join vote_info
    on voting_record.vote_id = vote_info.vote_id
