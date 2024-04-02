with committees_source as (
    SELECT * FROM {{ source('election', 'committees') }}
),
committee_types_source as (
    SELECT * FROM {{ source('election', 'committee_types') }}
),
pp_source as (
    SELECT * FROM {{ source('election', 'political_parties') }}
)

SELECT
    c.cmte_id, --Unique ID
    c.cmte_nm,
    c.cmte_city,
    c.cmte_st,
    c.cmte_zip,
    CASE
        WHEN cmte_dsgn = 'A' THEN 'Authorized by Candidate'
        WHEN cmte_dsgn = 'B' THEN 'Lobbyist/Registrant PAC'
        WHEN cmte_dsgn = 'D' THEN 'Leadership PAC'
        WHEN cmte_dsgn = 'J' THEN 'Joint Fundrasier'
        WHEN cmte_dsgn = 'P' THEN 'Principal Campaign Committee of the Candidate'
        WHEN cmte_dsgn = 'U' THEN 'Unauthorized'
        WHEN cmte_dsgn = NULL THEN cmte_dsgn
        ELSE cmte_dsgn
    END as cmte_dsgn,
    ct.cmte_typ,
    p.party_code_desc,
    CASE
        WHEN org_tp = 'C' THEN 'Administratively Terminated'
        WHEN org_tp = 'L' THEN 'Membership Organization'
        WHEN org_tp = 'T' THEN 'Trade Association'
        WHEN org_tp = 'V' THEN 'Cooperative'
        WHEN org_tp = 'W' THEN 'Cooperative w/o Capital Stock'
        WHEN org_tp = NULL THEN org_tp
        ELSE org_tp
    END as org_tp,
    c.connected_org_nm,
    c.cand_id
FROM committees_source c
INNER JOIN committee_types_source ct ON c.cmte_tp = ct.cmte_typ_code
INNER JOIN pp_source p ON c.cmte_pty_affiliation = p.party_code