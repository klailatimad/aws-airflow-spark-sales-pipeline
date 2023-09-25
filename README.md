# AWS Airflow Spark Sales Pipeline

An end-to-end data pipeline using AWS and Apache Airflow to process sales data, with analytics dashboarding via Apache Superset.

## Table of Contents

- [Overview](#overview)
- [System Architecture Overview](#system-architecture-overview)
- [Usage](#usage)
- [Installation](#installation)
- [Scripts](#scripts)

## Overview
End to end pipeline using AWS services (S3, Lambda, Cloudwatch, EMR, Glue, Athena) and Apache Airflow to process several files of input using Spark, and have them stored in a bucket for next day's usage and analytics. This also includes a BI tool, namely Apache Superset, to create a dashboard for visualizing and working on the business requirements.

## System Architecture Overview

For a detailed overview of the system architecture, see [this document](docs/SystemArchitecture.md).

### High-Level Overview

This project uses various components such as AWS S3, Snowflake, CloudWatch, Lambda, Airflow, EMR, Athena, Glue, and Superset to create a comprehensive data processing and analysis pipeline.

![Architecture_v3](https://github.com/klailatimad/midterm-project-aws-airflow/assets/122483291/209bd8be-7c92-465d-871a-2360b7aa5717)

## Usage

This section provides step-by-step instructions to set up and run the AWS Airflow Spark Sales Pipeline project.

### Preparing the Environment

1. **Clone the Repository**: Clone the GitHub repository to your local machine.

2. **Snowflake Setup**:
   - **Initialize Snowflake**: Ensure you have a Snowflake account and start your instance.
   - **Load Data**: Populate your Snowflake database with the necessary data.
   - **Create S3 Stage**: Configure an S3 stage in your Snowflake instance for data import/export.

3. **AWS Setup**:
   - **Configure AWS Account**: Ensure your AWS account is set up and configured properly.
   - **Create S3 Bucket**: Initialize an S3 bucket to store your data.

4. **Schedule Tasks**:
   - **Snowflake Scheduler**: Schedule a task in Snowflake to dump data into your S3 bucket.
   - **CloudWatch Schedule**: Use CloudWatch and a Cron expression to set up a schedule that will trigger your AWS Lambda functions.

### Pipeline Execution

5. **Lambda Functions**:
   - **File Readiness Check**: Scan the S3 bucket to check if all of today's files are ready. If they are, send a signal to Airflow to trigger the EMR.
   - **Notification**: If the files are not ready, Lambda sends an email notification using AWS SES service.

6. **IAM Role Configuration**: Create a new IAM Role for the EC2 instance so that it can communicate with the EMR cluster and the S3 buckets.

7. **Airflow Installation**: Install Apache Airflow on an AWS EC2 instance.

8. **Trigger EMR**: After receiving the signal from Lambda, Airflow unpacks the data and uses it as parameters for EMR.

9. **EMR Operations**:
   - **Data Read**: PySpark, running in EMR, reads data from the S3 bucket.
   - **Data Input**: Input files are in .csv format.
   - **Data Transformation**: It then transforms the data to generate dataframes that meet business requirements. Metrics such as total sales, average price, and stock levels are calculated.
   - **Data Output**: After the transformation, the final dataframe is saved as a parquet file to a new S3 output folder.

10. **Data Copy**: Copy additional files like store, product, and calendar details (which were previously dumped to the S3 bucket) to the new output folder.

11. **Athena and Glue Configuration**: Establish connections between Athena, Glue, and the S3 bucket that stores your final tables.

12. **Dashboarding and Reporting**: Use Apache Superset to connect to Athena and generate visualized reports as the final output.


## Installation

### Pre-requisites

- **AWS Account**: Sign up for an [AWS Account](https://aws.amazon.com/)
- **Docker**: Download and install [Docker](https://www.docker.com/products/docker-desktop)
- **Snowflake**: Create an account on [Snowflake](https://www.snowflake.com/)
- **Python 3.7**: Download and install [Python 3.7](https://www.python.org/downloads/)

### Steps

1. **Clone the Repository**
   - Use `git clone https://github.com/klailatimad/aws-airflow-spark-sales-pipeline/` to clone this repository to your local machine.

2. **Install Required Packages**
   - Manually install any dependencies you may need. If you're using Python, packages can be installed using `pip`.

3. **Set Up AWS Configuration**
   - Make sure your AWS credentials are set up properly for programmatic access. You can set this up through the [AWS Management Console](https://aws.amazon.com/console/) or by editing your `~/.aws/credentials` file.

4. **Initialize Snowflake Account**
   - If you haven't set up Snowflake, refer to the [Snowflake Documentation](https://docs.snowflake.com/en/user-guide/intro-key-concepts.html).

5. **Prepare S3 Bucket**
   - Follow the [AWS Documentation for S3 Bucket Setup](https://docs.aws.amazon.com/AmazonS3/latest/userguide/creating-bucket.html).

6. **Airflow Setup**
   - While there isn't a dedicated section on installing Airflow, you will need it for this project. You can refer to the [official Airflow installation guide](https://airflow.apache.org/docs/apache-airflow/stable/start.html) for instructions.

7. **Run the Pipeline**
   - Follow the instructions in [Usage](#usage) for pipeline execution, from setting up AWS Lambda functions to visualizing results in Apache Superset.

8. **Verify the Results**
   - Check the S3 bucket to confirm that the Parquet files have been saved correctly. 
   - Additionally, you can check the Apache Superset dashboard to see if the analytics are displaying as expected.

9. **Optional: Set up Monitoring**
   - Configure [CloudWatch alerts](https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/WhatIsCloudWatch.html) or any other monitoring tools you wish to use for real-time tracking of the pipeline's performance.


## Scripts

- [EMR_pyspark.py](EMR_pyspark.py): This script takes the date and file inputs, starts a SparkSession, maps each input file to a Spark dataframe, and performs the calculations required to answer the business questions. It joins the calculated tables and saves the result in parquet format in the output S3 bucket.
  - For details on the metrics calculated by this script, see [METRICS.md](/docs/METRICS.md).
- [lambda_function.py](lambda_function.py): Responsible for reading the files from the S3 bucket, comparing them against expected files. If matching, then send data to Airflow. If not matching, then send email using `send_email.py`.
- [project_dag.py](project_dag.py): Airflow Directed Acyclic Graph definition for parsing data from AWS Lambda and for utilizing EmrAddStepsOperator & EmrStepSensor.
- [send_email.py](send_email.py): Utility script for sending an email using AWS SES in case of a failed AWS Lambda function.
- [snowflake_db.sql](snowflake_db.sql): SQL commands for setting up Snowflake database for creating tables with schema, creating S3 stage, and S3 integration. It also creates a Snowflake task to be triggered every day at 2 am EST.
