USE election.public;

CREATE OR REPLACE STAGE individual_contributions_stage
URL='s3://fec-data/individual_contributions/'
CREDENTIALS=(AWS_KEY_ID='<AWS_KEY_ID>' AWS_SECRET_KEY='<AWS_SECRET_KEY>')
FILE_FORMAT = fec_csv;

SELECT
    $1 as "CMTE_ID",
    $2 as "AMNDT_IND",
    $3 as "RPT_TP",
    $4 as "TRANSACTION_PGI",
    $5 as "IMAGE_NUM",
    $6 as "TRANSACTION_TP",
    $7 as "ENTITY_TP",
    $8 as "NAME",
    $9 as "CITY",
    $10 as "STATE",
    $11 as "ZIP_CODE",
    $12 as "EMPLOYER",
    $13 as "OCCUPATION",
    $14 as "TRANSACTION_DT",
    $15 as "TRANSACTION_AMT",
    $16 as "OTHER_ID",
    $17 as "TRAN_ID",
    $18 as "FILE_NUM",
    $19 as "MEMO_CD",
    $20 as "MEMO_TEXT",
    $21 as "SUB_ID",
    METADATA$FILENAME  --Respective filename of the record
FROM @individual_contributions_stage
LIMIT 10;