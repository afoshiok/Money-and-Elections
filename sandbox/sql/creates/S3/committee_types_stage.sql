USE election.public;

CREATE OR REPLACE STAGE committee_types_stage
URL='s3://fec-data/committee_types/'
CREDENTIALS=(AWS_KEY_ID='<AWS_KEY_ID>' AWS_SECRET_KEY='<AWS_SECRET_KEY>')
FILE_FORMAT = fec_csv;