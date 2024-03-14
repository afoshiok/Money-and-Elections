USE election.public;

CREATE OR REPLACE STAGE operating_expenditures_stage
URL='s3://fec-data/operating_expenditures/'
CREDENTIALS=(AWS_KEY_ID='<AWS_KEY_ID>' AWS_SECRET_KEY='<AWS_SECRET_KEY>')
FILE_FORMAT = fec_csv;

SELECT
    $1 as "CMTE_ID",
    $2 as "AMNDT_IND",
    $3 as "RPT_YR",
    $4 as "RPT_TP",
    $5 as "IMAGE_NUM",
    $6 as "LINE_NUM",
    $7 as "FORM_TP_CD",
    $8 as "SCHED_TP_CD",
    $9 as "NAME",
    $10 as "CITY",
    $11 as "STATE",
    $12 as "ZIP_CODE",
    $13 as "TRANSACTION_DT",
    $14 as "TRANSACTION_AMT",
    $15 as "TRANSACTION_PGI",
    $16 as "PURPOSE",
    $17 as "CATEGORY",
    $18 as "CATEGORY_DESC",
    $19 as "MEMO_CD",
    $20 as "MEMO_TEXT",
    $21 as "ENTITY_TP",
    $22 as "SUB_ID",
    $23 as "FILE_NUM",
    $24 as "TRAN_ID",
    $25 as "BACK_REF_TRAN_ID"
FROM @operating_expenditures_stage
LIMIT 10;