CREATE TABLE IF NOT EXISTS cand_cmte_link (
    linkage_id number(12),
    cand_id varchar(9),
    cand_election_yr number(4),
    fec_election_yr number(4),
    cmte_id varchar(9),
    cmte_tp varchar(1),
    cmte_dsgn varchar(1)
)  