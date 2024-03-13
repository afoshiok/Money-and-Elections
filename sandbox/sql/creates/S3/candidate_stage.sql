--https://docs.snowflake.com/en/user-guide/data-load-s3-config-storage-integration
USE election.public;

CREATE OR REPLACE STAGE candidate_stage
URL='s3://fec-data/candidates/'
CREDENTIALS=(AWS_KEY_ID='<AWS_KEY_ID>' AWS_SECRET_KEY='<AWS_SECRET_KEY>')
FILE_FORMAT = fec_csv;

SELECT METADATA$FILENAME FROM @candidate_stage;

--Now let's check to see if the data is being read properly from S3, will be later used in the MERGE INTO scripts
SELECT
    $1 as "CAND_ID", 
    $2 as "CAND_NAME",
    $3 as "CAND_PTY_AFFILIATION",
    $4 as "CAND_ELECTION_YR",
    $5 as "CAND_OFFICE_ST",
    $6 as "CAND_OFFICE",
    $7 as "CAND_OFFICE_DISTRICT",
    $8 as "CAND_ICI",
    $9 as "CAND_STATUS",
    $10 as "CAND_PCC",
    $11 as "CAND_ST1",
    $12 as "CAND_ST2",
    $13 as "CAND_CITY",
    $14 as "CAND_ST",
    $15 as "CAND_ZIP",
    METADATA$FILENAME  --Respective filename of the record
FROM @candidate_stage
limit 10;