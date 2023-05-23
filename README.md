# Portfolio Project:
## End to end pipeline using AWS services (S3, Lambda, Cloudwatch, EMR, Glue, Athena) and Apache Airflow to process several files of input and store them for use for next day's usage. This also includes a BI tool, namely Superset, to output the results. 

![Architecture_v2](https://github.com/klailatimad/midterm-project-aws-airflow/assets/122483291/cac8e51e-327b-4275-a434-cde354194543)

### This project was created to show the ability to use different solutions replicating day to day ETL processes. The data is imported daily. The files imported were CSV files with millions of entries.

### Snowflake:
Snowflake was used as a transaction database in this project for testing purposes. It is used to import data from a public AWS S3 bucket and create the connection with my AWS S3 bucket and push the data to it.
 
### AWS S3:
S3 was used as a Data Lake for this solution. The data would be pushed from Snowflake on a daily basis and allow Lambda to connect to it.

### Cloudwatch + Lambda:
Cloudwatch was used to schedule Lambda to scan the S3 bucket and send a signal to Airflow when the lambda has judged that ‘today’s’ data is ready to be pushed.

Since there are multiple files dumped to S3 separately, it is not convenient to use S3 trigger for Lambda because we wouldn't know when to trigger the Lambda. Instead of using S3 event triggers for lambda, Cloudwatch is used  to schedule the Lambda function.

Cloudwatch schedule was created using Cron expression to plan when to trigger lambda (since raw data is dumped to S3 at 2 am every night, we can schedule triggers at 3 am, 3:30 am 2 times to trigger lambda) .

### Airflow:
Airflow is installed as a Docker in an AWS EC2 instance.

After Lambda sends a signal to Airflow, the Airflow will unpack the data from Lambda as the parameter for EMR. 

### EMR:
EMR is triggered by Airflow in the preceding step. EMR will use the files in the S3 bucket where raw data is dumped. Pyspark running in EMR will do the following tasks:

Task 1: Read data from S3.

Task 2: Perform data transformation process to generate a dataframe to meet the business requirements.

### Athena and Glue:
Athena and Glue will connect with the output datalake (AWS S3 bucket) to store the final fact and dimensions tables. This helps with standardization as data sources might change in the future.

### BI Tool (Superset):
Given options for the BI side, Superset run as docker was selected to output the results and create a dashboard that can be useful by the business.
