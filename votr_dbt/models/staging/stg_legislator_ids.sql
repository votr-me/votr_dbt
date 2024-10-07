-- models/staging/stg_legislator_ids.sql

{{ config(materialized='table') }}
select
    a.bioguide_id,
    coalesce(a.current_member, False) as is_current_member,
    coalesce(b.thomas_id, a.thomas_id) as thomas_id,
    coalesce(b.opensecrets_id, a.opensecrets_id) as opensecrets_id,
    coalesce(b.lis_id, a.lis_id) as lis_id,
    coalesce(b.cspan_id, a.cspan_id) as cspan_id,
    coalesce(b.govtrack_id, a.govtrack_id) as govtrack_id,
    coalesce(b.votesmart_id, a.votesmart_id) as votesmart_id,
    coalesce(b.ballotpedia_id, a.ballotpedia_id) as ballotpedia_id,
    coalesce(b.washington_post_id, a.washington_post_id) as washington_post_id,
    coalesce(b.icpsr_id, a.icpsr_id) as icpsr_id,
    coalesce(b.wikipedia_id, a.wikipedia_id) as wikipedia_id,
    string_to_array(trim(coalesce(b.fec_ids, a.fec_ids)), ',') as fec_ids
from {{ ref('legislators_ids_10_2_2024') }} as b
left join {{ source('raw','legislators') }} as a
    on b.bioguide_id = a.bioguide_id
