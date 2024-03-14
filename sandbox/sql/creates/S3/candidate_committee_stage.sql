USE election.public;

CREATE OR REPLACE STAGE candidate_committee_stage
URL='s3://fec-data/candidate-committee_link/'
CREDENTIALS=(AWS_KEY_ID='<AWS_KEY_ID>' AWS_SECRET_KEY='<AWS_SECRET_KEY>')
FILE_FORMAT = fec_csv;

SELECT
    $1 as "CAND_ID",
    $2 as "CAND_ELECTION_YR",
    $3 as "FEC_ELECTION_YR",
    $4 as "CMTE_ID",
    $5 as "CMTE_TP",
    $6 as "CMTE_DSGN",
    $7 as "LINKAGE_ID"
FROM @candidate_committee_stage
LIMIT 10;