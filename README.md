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
2. DAGs that scrape data straight from the tables on the FEC website. These DAGs are reserved for data used in "type" tables (i.e. Report Type, Transaction Type, and Political Parties).

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
