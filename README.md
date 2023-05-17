# Portfolio Project:
## End to end pipeline using AWS services (S3, Lambda, Cloudwatch, EMR, Glue, Athena) and Airflow to process several files of input and store them for use for next day's usage. This also includes a BI tool, namely Superset, to outpt the results. 

![Architecture](https://github.com/klailatimad/midterm-project-aws-airflow/assets/122483291/649e2428-4861-47c4-ac69-15bcd895e83e)

### This project was created to show the ability to use different solutions replicating day to day ETL processes. The data is imported on a daily basis. The files imported were CSV files with millions of entries.

#### Transaction database (Snowflake used in this project for testing purposes) was used to import data from a public AWS S3 bucket and create the connection with my AWS S3 bucket.
 
#### AWS S3 was used as a Data Lake for this solution.

#### Cloudwatch was used to schedule Lambda to scan the S3 bucket, and send a signal to Airflow when the lambda has judged that ‘today’s’ data is ready to be pushed.
##### Since we have multiple files dumped to S3 separately, it is not very convenient to use S3 trigger for Lambda because we don’t know when to trigger the Lambda. Instead of using S3 event triggers for lambda, we use Cloudwatch to schedule the Lambda function.
##### Cloudwatch schedule was created useig Cron expression to plan when to trigger lambda (since raw data is dumped to S3 at 2 am every night, we can schedule triggers at 3 am, 3:30 am 2 times to trigger lambda) .

#### After Lamda sends a signal to Airflow, the Airflow will unpack the data from Lambda as the parameter for EMR.

#### EMR is triggered by Airflow in the preceding step. EMR will use the files in the S3 bucket where raw data is dumped. Pyspark running in EMR will do the following tasks:
###### Task 1: Read data from S3.
###### Task 2: Do data transformation process to generate a dataframe to meet the following business requirement.

#### Athena and Glue will connect with the output datalake (AWS S3 bucket) to store the final fact and dimensions tables. This helps with standardization as data sources might change in the future.

#### Given options for the BI side, Superset run as docker was selected to output the results and create a dashboard that can be useful by the business.
