USE election.public;

CREATE OR REPLACE STAGE political_parties_stage
URL='s3://fec-data/political_parties/'
CREDENTIALS=(AWS_KEY_ID='<AWS_KEY_ID>' AWS_SECRET_KEY='<AWS_SECRET_KEY>')
FILE_FORMAT = fec_csv;

SELECT
    $1 as "PTY_CODE",
    $2 as "PTY_DESC",
    $3 as "NOTES"
FROM @political_parties_stage
LIMIT 10;

