# System Architecture Overview
This document outlines the different components used in the data processing and analysis pipeline. Each component serves a specific purpose, contributing to the end goal of transforming raw data into actionable business insights. The architecture comprises various AWS services, data processing frameworks, and a BI tool.

## Data Storage and Import
### Snowflake
Purpose: Serves as a transaction database for testing purposes.
Functionality: Imports data from a public AWS S3 bucket and pushes it to another AWS S3 bucket for storage.
### AWS S3 (Data Lake)
Purpose: Acts as a centralized data lake.
Functionality: Stores raw data imported from Snowflake and serves as a data source for Lambda and EMR.

## Monitoring and ETL Initiation
### CloudWatch and Lambda
Purpose: Monitor the S3 bucket and initiate the ETL (Extract, Transform, Load) process.
Trigger: Utilizes Cron expressions in CloudWatch to schedule Lambda functions.
Functionality: Scans the AWS S3 bucket and sends a signal to Airflow to start the ETL process.

## Data Processing
### Apache Airflow
Deployment: Installed as a Docker container on an AWS EC2 instance.
Trigger: Activated by Lambda to initiate data processing in EMR.
### Amazon EMR
Trigger: Initiated by Airflow.

Tasks:

Reads raw data from the S3 bucket.
Performs data transformation using PySpark to generate dataframes based on specified business requirements.
Saves the final dataframe as a Parquet file in a new S3 output folder (or bucket) and copies related metadata files.
Business Requirements: The final dataframe will include various calculated metrics such as total sales quantity, average sales price, and stock levels, among others.

Data Analysis and Reporting
Athena and Glue
Purpose: Standardizes data sources for future-proofing and enables querying.
Glue: Scans the schema of the Parquet dataframe.
Athena: Used for querying data and acts as the data loading point for the BI tool.
BI Tool (Superset)
Deployment: Runs as a Docker container.
Functionality: Scans schema using Athena and enables the creation of dashboards based on business queries.
