--In order to create these users and roles, use your admin role
USE ROLE ACCOUNTADMIN;

--Following best practices, I'll be creating a role for transforming users transforming the data
CREATE ROLE IF NOT EXISTS transform;
GRANT OPERATE ON WAREHOUSE /* Add your warehouse */ TO ROLE TRANSFORM;

--Creating 'dbt' user and assigning it to the 'transform' role

CREATE USER IF NOT EXISTS dbt
    PASSWORD='YOUR PASSWORD'
    LOGIN_NAME='YOUR USER NAME' --Snowflake specific
    MUST_CHANGE_PASSWORD=FALSE --Snowflake specific
    DEFAULT_WAREHOUSE='ELECTION' --Snowflake specific
    DEFAULT_ROLE='transform' --Snowflake specific
    DEFAULT_NAMESPACE='ELECTION.RAW' --Snowflake specific
    COMMENT='User for DBT data transformation'; --Snowflake specific

GRANT ROLE transform to USER dbt;

CREATE DATABASE IF NOT EXISTS ELECTION;
CREATE SCHEMA IF NOT EXISTS ELECTION.RAW;

GRANT ALL ON WAREHOUSE /* Add your warehouse */ TO ROLE transform;
GRANT ALL ON DATABASE ELECTION TO ROLE transform;
GRANT ALL ON ALL SCHEMAS IN DATABASE ELECTION TO ROLE transform;
GRANT ALL ON FUTURE SCHEMAS IN DATABASE ELECTION TO ROLE transform;
GRANT ALL ON ALL TABLES IN SCHEMA ELECTION.RAW TO ROLE transform;
GRANT ALL ON FUTURE TABLES IN SCHEMA ELECTION.RAW TO ROLE transform;

--DEFAULTS

USE WAREHOUSE /* Add your warehouse */ ;
USE DATABASE ELECTION;
USE SCHEMA ELECTION.RAW