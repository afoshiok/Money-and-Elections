# Money + Elections (End-to-End Data Engineering Project)

## At-A-Glance

Election year has finally rolled back around. You know what that means? Millions, if not billions, of dollars are being spent on sending candidates to Capitol Hill and the White House. In 1974, the Federal Election Commission (FEC) was created to oversee every federal election in the United States. As a result of this, every candidate's financial records were made openly accessible to the public. This project will use FEC data to create a dashboard monitoring all election finances in the 2024 election cycle.

## Project Diagram (Click to make it readable)
![image](https://github.com/afoshiok/Money-and-Elections/assets/89757138/77615769-dd93-4149-a74e-df885caf6d81)

## ERD
After figuring out what data I would need, I made an ERD based on the data documentation provided by the [FEC](https://www.fec.gov/data/browse-data/?tab=bulk-data). Each table has an ingestion DAG to move the data from the CSVs to an S3 bucket.


![2024 Election ERD](https://github.com/afoshiok/Money-and-Elections/assets/89757138/c6408dd5-978a-45c2-86a3-a214682e15a5)

## Data Stack
**Data Orchestration:**
- Apache Airflow

**Data Processing/Cleaning:**
- Polars
- Pandas

**Data Warehouse and Staging:**
- AWS S3
- Snowflake

**Data Transformations:**
- DBT (Data Build Tool)
- SQL (of course)

**Data Visualization / BI:**
- Metabase

**Other:**
- AWS IAM (Access Management for AWS)
- Notion (Project management / Documentation)
- Docker (App containerization)

## Data Ingestion

There are two types of ingestion DAGs:

1. DAGs that download the bulk data from the FEC website.
2. DAGs that scrape data straight from the tables on the FEC website. These DAGs are reserved for data in "type" tables (i.e. Report Type, Transaction Type, and Political Parties).

**Example of a "Bulk Data" DAG (TLDR - Full code: [here](https://github.com/afoshiok/Money-and-Elections/blob/main/airflow/dags/ingestion/candidates.py)):**

For these types of DAGs, I chose to convert the files into parquet files because it decreased my DAG runtimes and cut costs on S3 storage (Only $5, but hey money is money!). For example for the "Individual Contributions" DAG, the runtime went from ****15m 57s → 5m 25s** and the file size went from **3.4 GB → 528.7 MB**

 ** = Not including the DAG trigger.
![image](https://github.com/afoshiok/Money-and-Elections/assets/89757138/cc7c0186-fa9b-451b-b188-e4b113c369e6)


**Example of a "Web Scraper" DAG (TLDR - Full code: [here](https://github.com/afoshiok/Money-and-Elections/blob/main/airflow/dags/ingestion/report_types.py)):**

![image](https://github.com/afoshiok/Money-and-Elections/assets/89757138/9eb14128-a293-448d-b7a9-151c60e66b48)

## Loading Data into Snowflake

Before loading the data into the data warehouse, I had to make the tables (see ERD) and create an S3 stage for each table. To make the S3 stages, allow Snowflake access to my "./FEC" S3 Bucket. I created a custom role for Snowflake in AWS IAM and created the stages using a script like this:

![image](https://github.com/afoshiok/Money-and-Elections/assets/89757138/718c8f42-505c-48c0-949f-85410ff8b382)

\* Snowflake recommends using CREATE INTEGRATION to give access to S3 instead of using the AWS ID and AWS SECRET KEY!

A corresponding COPY INTO DAG is triggered at the end of both types of ingestion DAGs in the previous section. These DAGs follow these steps:

1. The DAG receives a run date as a parameter.
2. Truncate the table in Snowflake to avoid duplicate data.
3. Copy the data into the table using the run date parameter.
4. Run DBT models*

  \* Didn't add it because I use DBT locally instead of DBT cloud, and Airflow wasn't cooperating with DBT.

Here's an example using the Candidates table:

![image](https://github.com/afoshiok/Money-and-Elections/assets/89757138/b30e2311-ae6d-41cb-af24-1bd1a7067107)

## Data Transformation (DBT)

I used DBT to make data models to be used by Metabase. All my sources tables are placed in the "Election.Raw" schema, and all the models created with DBT are placed in the "Election.Analytics" schema. Below you will see the lineage graphs of the "src" models and the "fct" views they make. If you want to look at the full DBT project [look here](https://github.com/afoshiok/Money-and-Elections/tree/main/airflow/dbt/election_dbt).

**Lineage Graphs (Click to make readable):**

| src_candidates    | src_committee_transactions |
| -------- | ------- |
![image](https://github.com/afoshiok/Money-and-Elections/assets/89757138/81be76e9-e9cf-4a20-8497-d3730577bfbc) | ![image](https://github.com/afoshiok/Money-and-Elections/assets/89757138/7c7bada4-98a4-4336-8f36-ab73297a9195)

| src_committees | src_independent_exp |
| -------- | ------- |
![image](https://github.com/afoshiok/Money-and-Elections/assets/89757138/0550e029-68ee-46b9-9ca7-6079a4019718) | ![image](https://github.com/afoshiok/Money-and-Elections/assets/89757138/f42e515c-2719-4529-8a26-5f71a069ac12)

| src_individual_cont | src_operating_exp |
| -------- | ------- |
![image](https://github.com/afoshiok/Money-and-Elections/assets/89757138/5d9a3d49-ad56-4f86-a278-8b53d5ad6fc7) | ![image](https://github.com/afoshiok/Money-and-Elections/assets/89757138/c4caaf59-231a-40ee-94f4-4618fb0e0760)


