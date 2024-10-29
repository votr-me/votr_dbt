-- models/staging/stg_legislator_ids.sql

{{ config(materialized='table') }}
select
    a.bioguide_id,
    COALESCE(a.current_member, FALSE) AS is_current_member,
    COALESCE(b.thomas_id, a.thomas_id) AS thomas_id,
    COALESCE(b.opensecrets_id, a.opensecrets_id) AS opensecrets_id,
    COALESCE(b.lis_id, a.lis_id) AS lis_id,
    COALESCE(b.cspan_id, a.cspan_id) AS cspan_id,
    COALESCE(b.govtrack_id, a.govtrack_id) AS govtrack_id,
    COALESCE(b.votesmart_id, a.votesmart_id) AS votesmart_id,
    COALESCE(b.ballotpedia_id, a.ballotpedia_id) AS ballotpedia_id,
    COALESCE(b.washington_post_id, a.washington_post_id) AS washington_post_id,
    COALESCE(b.icpsr_id, a.icpsr_id) AS icpsr_id,
    COALESCE(b.wikipedia_id, a.wikipedia_id) AS wikipedia_id,
    SPLIT(TRIM(COALESCE(b.fec_ids, a.fec_ids)), ',') AS fec_ids
from {{ ref('legislators_ids_10_2_2024') }} as b
left join {{ source('raw','legislators') }} as a
    on b.bioguide_id = a.bioguide_id
