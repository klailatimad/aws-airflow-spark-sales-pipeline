## End to end pipeline using AWS services (S3, Lambda, Cloudwatch, EMR, Glue, Athena) and Apache Airflow to process several files of input using Spark, and have them stored in a bucket for next day's usage and analytics. This also includes a BI tool, namely Apache Superset, to create a dashboard for visualizing and working on the business requirements.

![Architecture_v3](https://github.com/klailatimad/midterm-project-aws-airflow/assets/122483291/209bd8be-7c92-465d-871a-2360b7aa5717)

### This architecture mimics some companies’ ETL process: data files are sent to the company's data lake (S3 bucket) everyday from an internal transaction database. The company first needs to scan the S3 bucket. When the files are ready, the ETL process begins.

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
Task 2: Perform data transformation process to generate a dataframe to meet the following business requirements.

#### The table will be grouped by each week, each store, each product to calculate the following metrics:

- total sales quantity of a product : Sum(sales_qty)
- total sales amount of a product : Sum(sales_amt) 
- average sales Price: Sum(sales_amt)/Sum(sales_qty) 
- stock level by the end of the week : stock_on_hand_qty by the end of the week (only the stock level at the end day of the week)
- store on Order level by then end of the week: ordered_stock_qty by the end of the week (only the ordered stock quantity at the end day of the week)
- total cost of the week: Sum(cost_amt)
- the percentage of Store In-Stock: (how many times of out_of_stock in a week) / days of a week (7 days) 
- total Low Stock Impact: sum (out_of+stock_flg + Low_Stock_flg*)  
- potential Low Stock Impact: if Low_Stock_Flg* =TRUE then SUM(sales_amt - stock_on_hand_amt) 
- no Stock Impact: if out_of_stock_flg=true, then sum(sales_amt)
- low Stock Instances: Calculate how many times of Low_Stock_Flg* in a week
- no Stock Instances: Calculate then how many times of out_of_Stock_Flg in a week 
- how many weeks the on hand stock can supply: (stock_on_hand_qty at the end of the week) / sum(sales_qty)
###### *(Low Stock_flg = if today's stock_on_hand_qty<sales_qty , then low_stock_flg=1, otherwise =0) * 

Task 3: After the transformation is completed, the final dataframe is saved as a parquet file to a new S3 output folder (or a new bucket).
Also  the files store, product and calendar input files are copied to the new output folder. 
The new parquet file together with the store, product and calendar files in the output folder will be used for later data analysis with Athena.

### Athena and Glue:
Athena and Glue will connect with the output datalake (AWS S3 bucket) to store the final fact and dimensions tables. This helps with standardization as data sources might change in the future.
#### Glue:
The Glue crawler is creatged to scan the schema of the parquet dataframe.

#### Athena:
Athena is used to query the data and becomes a loading point for the data before it is queried by Superset

### BI Tool (Superset):
Given options for the BI side, Superset run as docker was selected to output the results and create a dashboard that can be useful by the business.
Superset scans the schema using Athena and allows for a dashboard to be created based on the questions that were answered in this project.
