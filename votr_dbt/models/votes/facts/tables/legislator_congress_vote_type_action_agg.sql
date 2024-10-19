{{ config(
    materialized='incremental'
) }}

with vote_type_counts as ( 
    SELECT 
        congress,
        bioguide_id,
        CASE 
            WHEN legislator_vote in ('Aye', 'Yea') THEN 'Yes'
            WHEN legislator_vote in ('No', 'Nay') THEN 'No'
            ELSE legislator_vote
        END as legislator_vote,
        count(distinct vote_id) as n_legislator_votes,
        count(distinct case when vote_type='amendment' then vote_id else null end) as n_amendment_votes,
        count(distinct case when vote_type='cloture' then vote_id else null end) as n_cloture_votes,
        count(distinct case when vote_type='conviction' then vote_id else null end) as n_conviction_votes,
        count(distinct case when vote_type='impeachment' then vote_id else null end) as n_impeachment_votes,
        count(distinct case when vote_type='leadership' then vote_id else null end) as n_leadership_votes,
        count(distinct case when vote_type='nomination' then vote_id else null end) as n_nomination_votes,
        count(distinct case when vote_type='passage' then vote_id else null end) as n_passage_votes,
        count(distinct case when vote_type='passage-suspension' then vote_id else null end) as n_passage_suspension_votes,
        count(distinct case when vote_type='procedural' then vote_id else null end) as n_procedural_votes,
        count(distinct case when vote_type='quorum' then vote_id else null end) as n_quorum_votes,
        count(distinct case when vote_type='recommit' then vote_id else null end) as n_recommit_votes,
        count(distinct case when vote_type='treaty' then vote_id else null end) as n_treaty_votes,
        count(distinct case when vote_type='unknown' then vote_id else null end) as n_unknown_votes,
        count(distinct case when vote_type='veto-override' then vote_id else null end) as n_veto_override_votes
    from {{ ref('dim_legislator_voting_record')}}
    group by 1, 2, 3
),

vote_type_percentages as (
    select 
        congress,
        bioguide_id,
        legislator_vote,
        n_amendment_votes / nullif(n_legislator_votes, 0) as pct_amendment_votes,
        n_cloture_votes / nullif(n_legislator_votes, 0) as pct_cloture_votes,
        n_conviction_votes / nullif(n_legislator_votes, 0) as pct_conviction_votes,
        n_impeachment_votes / nullif(n_legislator_votes, 0) as pct_impeachment_votes,
        n_leadership_votes / nullif(n_legislator_votes, 0) as pct_leadership_votes,
        n_nomination_votes / nullif(n_legislator_votes, 0) as pct_nomination_votes,
        n_passage_votes / nullif(n_legislator_votes, 0) as pct_passage_votes,
        n_passage_suspension_votes / nullif(n_legislator_votes, 0) as pct_passage_suspension_votes,
        n_procedural_votes / nullif(n_legislator_votes, 0) as pct_procedural_votes,
        n_quorum_votes / nullif(n_legislator_votes, 0) as pct_quorum_votes,
        n_recommit_votes / nullif(n_legislator_votes, 0) as pct_recommit_votes,
        n_treaty_votes / nullif(n_legislator_votes, 0) as pct_treaty_votes,
        n_unknown_votes / nullif(n_legislator_votes, 0) as pct_unknown_votes,
        n_veto_override_votes / nullif(n_legislator_votes, 0) as pct_veto_override_votes
    from vote_type_counts
)

select 
    vote_type_counts.*, 
    vote_type_percentages.pct_amendment_votes,
    vote_type_percentages.pct_cloture_votes,
    vote_type_percentages.pct_conviction_votes,
    vote_type_percentages.pct_impeachment_votes,
    vote_type_percentages.pct_leadership_votes,
    vote_type_percentages.pct_nomination_votes,
    vote_type_percentages.pct_passage_votes,
    vote_type_percentages.pct_passage_suspension_votes,
    vote_type_percentages.pct_procedural_votes,
    vote_type_percentages.pct_recommit_votes,
    vote_type_percentages.pct_treaty_votes,
    vote_type_percentages.pct_unknown_votes,
    vote_type_percentages.pct_veto_override_votes
from vote_type_counts inner join vote_type_percentages
    on vote_type_counts.bioguide_id = vote_type_percentages.bioguide_id
    and vote_type_counts.congress = vote_type_percentages.congress
    and vote_type_counts.legislator_vote = vote_type_percentages.legislator_vote

