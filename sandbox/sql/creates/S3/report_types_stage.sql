USE election.public;

CREATE OR REPLACE STAGE report_types_stage
URL='s3://fec-data/report_types/'
CREDENTIALS=(AWS_KEY_ID='<AWS_KEY_ID>' AWS_SECRET_KEY='<AWS_SECRET_KEY>')
FILE_FORMAT = fec_csv;

SELECT
    $1 as "RP_TYP_CODE",
    $2 as "RP_TYP",
    $3 as "NOTES"
FROM @report_types_stage
LIMIT 10;