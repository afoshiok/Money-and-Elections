USE election.public;

CREATE OR REPLACE STAGE transaction_types_stage
URL='s3://fec-data/transaction_types/'
CREDENTIALS=(AWS_KEY_ID='<AWS_KEY_ID>' AWS_SECRET_KEY='<AWS_SECRET_KEY>')
FILE_FORMAT = fec_csv;

SELECT
    $1 as "TRANS_TP",
    $2 as "TRANS_TP_DESC"
FROM @transaction_types_stage
LIMIT 10;