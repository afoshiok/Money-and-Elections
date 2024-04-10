with source as (
    SELECT * FROM {{ source('election', 'candidates') }}
),
pp_source as (
    SELECT * FROM {{ source('election', 'political_parties') }}
)

SELECT 
    s.cand_id,
    s.cand_name,
    p.party_code_desc,
    s.cand_election_yr,
    s.cand_office_st,
    s.cand_office,
    CASE WHEN cand_office_district IS NOT NULL
        THEN cand_office_st || cand_office_district
        ELSE cand_office_district
    END as cand_office_district,
    CASE 
        WHEN s.cand_ici = 'I' THEN 'Incumbent'
        WHEN s.cand_ici = 'C' THEN 'Challenger'
        WHEN s.cand_ici = 'O' THEN 'Open Seat'
    END as cand_ici,
    s.cand_status,
    s.cand_city,
    s.cand_st
 FROM source s
 INNER JOIN pp_source p ON s.cand_pty_affiliation = p.party_code