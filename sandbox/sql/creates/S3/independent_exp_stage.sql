USE election.public;

CREATE OR REPLACE STAGE independent_expenditures_stage
URL='s3://fec-data/independent_expenditures/'
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
    $17 as "CAND_ID",
    $18 as "TRAN_ID",
    $19 as "FILE_NUM",
    $20 as "MEMO_CD",
    $21 as "MEMO_TEXT",
    $22 as "SUB_ID"
FROM @independent_expenditures_stage
LIMIT 10;