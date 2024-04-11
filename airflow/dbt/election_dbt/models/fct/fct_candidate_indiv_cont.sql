with cand_source as (
    SELECT * FROM {{ ref('src_candidates') }}
    WHERE cand_election_yr = 2024 --Just looking at active 2024 candidates
),

ccl_source as (
    SELECT * FROM {{ source('election', 'cand_cmte_link') }}
    WHERE cand_election_yr = 2024
),

indiv_cont_source as (
    SELECT * FROM {{ ref('src_indiv_contributions') }}
)

SELECT
    cs.cand_name,
    cs.party_code_desc,
    cs.cand_office_st,
    CASE
        WHEN cs.cand_office = 'H' THEN 'House'
        WHEN cs.cand_office = 'S' THEN 'Senate'
        WHEN cs.cand_office = 'P' THEN 'President'
        ELSE cs.cand_office
    END as cand_office,
    cs.cand_office_district,
    SUM(ics.transaction_amt) as total_contributions
FROM cand_source cs
INNER JOIN ccl_source ccl ON cs.cand_id = ccl.cand_id
INNER JOIN indiv_cont_source ics on ccl.cmte_id = ics.cmte_id
GROUP BY cs.cand_name, cs.party_code_desc, cs.cand_office_st, cs.cand_office, cs.cand_office_district