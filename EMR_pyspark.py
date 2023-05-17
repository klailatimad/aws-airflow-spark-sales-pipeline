import sys
from pyspark.sql import SparkSession

date_str = sys.argv[1]
file_input = sys.argv[2]

# Create SparkSession

spark = SparkSession.builder.master("local[*]").appName("demo").getOrCreate()

sales_df = (
    spark.read.option("header","true").option("delimiter",",")
    # .csv(f"file:////home/ec2-user-midterm/data/sales_{data_str}.csv.gv")
    # .csv(f"s3:/wcddev4-wh-dump/sales_{date_str}.csv.gv")
    .csv(f"{file_input}")
)

sales_df.createOrReplaceTempView("sales")

df_sum_sales_qty = spark.sql(
    "select trans_dt,store_key,prod_key,sum(sales_qty) as sum_sales_qty from sales group by 1,2"
)

df_sum_sales_qty.show()

(
    df_sum_sales_qty.repartition(1)
    .write.mode("overwrite")
    .option("compression","gzip")
    # .parquet(f"s3a://wcddev6-midterm-output/sales/date={date_str})
    .parquet(f"file:////home/ubuntu/midterm/data/result/date={date_str}")
)
