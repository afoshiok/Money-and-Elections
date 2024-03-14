USE election.public;

CREATE OR REPLACE STAGE committee_stage
URL='s3://fec-data/committees/'
CREDENTIALS=(AWS_KEY_ID='<AWS_KEY_ID>' AWS_SECRET_KEY='<AWS_SECRET_KEY>')
FILE_FORMAT = fec_csv;

SELECT
    $1 as "CMTE_ID",
    $2 as "CMTE_NM",
    $3 as "TRES_NM",
    $4 as "CMTE_ST1",
    $5 as "CMTE_ST2",
    $6 as "CMTE_CITY",
    $7 as "CMTE_ST",
    $8 as "CMTE_ZIP",
    $9 as "CMTE_DSGN",
    $10 as "CMTE_TP",
    $11 as "CMTE_PTY_AFFILIATION",
    $12 as "CMTE_FILING_FREQ",
    $13 as "ORG_TP",
    $14 as "CONNECTED_ORG_NM",
    $15 as "CAND_ID",
    METADATA$FILENAME  --Respective filename of the record
FROM @committee_stage
limit 10;
