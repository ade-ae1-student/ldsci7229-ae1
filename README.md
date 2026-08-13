# LDSCI7229 Advanced Data Engineering - AE1

Serverless data pipeline integrating batch and streaming sources on AWS.

## Architecture 

CSV upload and CrossRef API to S3, transformed by Glue, queried through
Athena, visualised in OpenSearch Dashboards. Orchestrated by Step Functions
with run metadata recorded in DynamoDB.

## Files

| File | Purpose |
|---|---|
| crossref_producer.py | Lambda producer: CrossRef REST API to Kinesis Data Firehose |
| glue_raw_to_cleaned.py | Glue ETL: cleans, validates and merges both sources into partitioned Parquet |
| statemachine.json | Step Functions definition: Lambda, crawler, Glue ETL, Athena |
| athena_queries.sql | Analytical queries with recorded scan volumes |

## Environment

AWS Academy Learner Lab, us-east-1, LabRole only. No custom IAM or VPC.
