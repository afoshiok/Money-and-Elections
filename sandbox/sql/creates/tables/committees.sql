CREATE TABLE IF NOT EXISTS raw_committees (
    cmte_id varchar(9),
    cmte_nm varchar(200),
    tresnam varchar(90),
    cmte_st1 varchar(34),
    cmte_st2 varchar(34),
    cmte_city varchar(30),
    cmte_st varchar(2),
    cmte_zip varchar(9),
    cmte_dsgn varchar(1),
    cmte_tp varchar(1),
    cmte_pty_affiliation varchar(3),
    cmte_filing_freq varchar(1),
    org_tp varchar(1),
    connected_org_nm varchar(200),
    cand_id varchar(9) 
)  