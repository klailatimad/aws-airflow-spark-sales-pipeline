# Import the necessary libraries
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, last, mean, col, avg, when, lit, to_date, row_number, count
from pyspark.sql.window import Window
from pyspark.sql import functions as F

# Define the date and file inputs
date_str = sys.argv[1]
file_inputs = sys.argv[2].split(',')

# Create SparkSession
# spark = SparkSession.builder.master("yarn").appName("demo").getOrCreate()  # for production
spark = SparkSession.builder.master("local[*]").appName("demo").getOrCreate()  # for testing

# Define the file paths and DataFrame names in a dictionary
file_mapping = {
        "calendar_df": file_inputs[0],
        "inventory_df": file_inputs[1],
        "product_df": file_inputs[2],
        "sales_df": file_inputs[3],
        "store_df": file_inputs[4]    
    }

# Read each file into the corresponding DataFrame
for k, v in file_mapping.items():
    file_mapping[k] = spark.read.csv(v, header=True, inferSchema=True)

# Assign each DataFrame to its corresponding variable
inventory_df = file_mapping["inventory_df"]
sales_df = file_mapping["sales_df"]
store_df = file_mapping["store_df"]
product_df = file_mapping["product_df"]
calendar_df = file_mapping["calendar_df"]

# change data type of CAL_DT in inventory_df to DateType
inventory_df = inventory_df.withColumn("CAL_DT", to_date(inventory_df["CAL_DT"], "yyyy-MM-dd"))

# change data type of NEXT_DELIVERY_DT in inventory_df to DateType
inventory_df = inventory_df.withColumn("NEXT_DELIVERY_DT", to_date(inventory_df["NEXT_DELIVERY_DT"], "yyyy-MM-dd"))

# change data type of TRANS_DT in sales_df to DateType
sales_df = sales_df.withColumn("TRANS_DT", to_date(sales_df["TRANS_DT"], "yyyy-MM-dd"))

# change data type of CAL_DT in calendar_df to DateType
calendar_df = calendar_df.withColumn("CAL_DT", to_date(calendar_df["CAL_DT"], "yyyy-MM-dd"))

# group the dataframes together for each week, each store, each product using TRANS_DT == CAL_DT, PROD_KEY, STORE_KEY
merged_df = sales_df.join(inventory_df, ["PROD_KEY", "STORE_KEY"]) \
                    .withColumnRenamed("CAL_DT", "INVENTORY_CAL_DT") \
                    .join(calendar_df, sales_df["TRANS_DT"] == calendar_df["CAL_DT"]) \
                    .join(store_df, "STORE_KEY") \
                    .join(product_df, "PROD_KEY")

# merged_df.show()

# calculate the total sales amount, total sales quantity, average sales price, and total cost of the week
# group by each week (use YR_WK_NUM from merged_df), each store, each product
df_sum_sales_amt_qty_cost = merged_df.groupBy("YR_WK_NUM", "STORE_KEY", "PROD_KEY") \
                                    .agg(sum("sales_amt").alias("total_sales_amt"), sum("sales_qty").alias("total_sales_qty"), avg("sales_amt").alias("avg_sales_price"), sum("sales_cost").alias("total_cost")) \
                                    .orderBy("YR_WK_NUM", "STORE_KEY", "PROD_KEY")


# df_sum_sales_amt_qty_cost.show()

# calculate stock level by the end of the week : stock_on_hand_qty by the end of the week (only the stock level at the end day of the week)
# join calendar table and use window function to get the last day of the week (DAY_OF_WK_NUM = 0, Sunday)

df_stock_level = merged_df.withColumn("row_num", F.row_number().over(Window.partitionBy("YR_WK_NUM", "STORE_KEY", "PROD_KEY").orderBy(F.desc("CAL_DT")))) \
                            .where(F.col("row_num") == 1) \
                            .select("YR_WK_NUM", "STORE_KEY", "PROD_KEY", "INVENTORY_ON_HAND_QTY") \
                            .withColumnRenamed("INVENTORY_ON_HAND_QTY", "INVENTORY_ON_HAND_QTY_END_WEEK")

# df_stock_level.show()

# calculate stock on order level by the end of the week: 
# only the ordered stock quantity at the end day of the week
# join calendar table and use window function to get the last day of the week (DAY_OF_WK_NUM = 0, Sunday)

df_stock_on_order = merged_df.withColumn("row_num", F.row_number().over(Window.partitionBy("YR_WK_NUM", "STORE_KEY", "PROD_KEY").orderBy(F.desc("CAL_DT")))) \
                            .where(F.col("row_num") == 1) \
                            .select("YR_WK_NUM", "STORE_KEY", "PROD_KEY", "INVENTORY_ON_ORDER_QTY") \
                            .withColumnRenamed("INVENTORY_ON_ORDER_QTY", "INVENTORY_ON_ORDER_QTY_END_WEEK")

# df_stock_on_order.show()

# calculate total cost of the week 
df_sum_cost = merged_df.groupBy("YR_WK_NUM", "STORE_KEY", "PROD_KEY") \
                        .agg(sum("sales_cost").alias("total_cost_week")) \
                        .orderBy("YR_WK_NUM", "STORE_KEY", "PROD_KEY")


# calculate the percentage of Store In-Stock: (how many times of out_of_stock in a week) / days of a week (7 days)
# divide sum(out_of_stock_flg) by 7

df_out_of_stock = merged_df.groupBy("YR_WK_NUM", "STORE_KEY", "PROD_KEY") \
                            .agg(sum("out_of_stock_flg").alias("out_of_stock_count")) \
                            .withColumn("out_of_stock_count", col("out_of_stock_count") / 7) \
                            .select("YR_WK_NUM", "STORE_KEY", "PROD_KEY", "out_of_stock_count") \
                            .orderBy("YR_WK_NUM", "STORE_KEY", "PROD_KEY")

# df_out_of_stock.show()

# calculate Low Stock_flg = if today's stock_on_hand_qty<sales_qty , then low_stock_flg=1, otherwise =0
df_low_stock = merged_df.withColumn("low_stock_flg", when(col("INVENTORY_ON_HAND_QTY") < col("SALES_QTY"), 1).otherwise(0)) \
                        .select("CAL_DT", "YR_WK_NUM", "STORE_KEY", "PROD_KEY", "low_stock_flg")

# df_low_stock.show()

# create temporary view for df_low_stock using spark.sql
df_low_stock.createOrReplaceTempView("df_low_stock")

# calculate total Low Stock Impact: sum (out_of_stock_flg  + Low_Stock_flg) for each week, each store, each product using spark.sql.functions

# Subquery m
window_spec_m = Window.partitionBy("STORE_KEY", "PROD_KEY", "CAL_DT").orderBy("CAL_DT")
m = merged_df.withColumn("row_num", row_number().over(window_spec_m))
m = m.filter(col("out_of_stock_flg") == 1)
m = m.groupBy("YR_WK_NUM", "STORE_KEY", "PROD_KEY").agg(count("out_of_stock_flg").alias("out_of_stock_count"))

# Subquery t2
t2 = df_low_stock.groupBy("STORE_KEY", "PROD_KEY").agg(count("low_stock_flg").alias("low_stock_count"))

# Join and final query
df_low_stock_impact = m.join(t2, ["STORE_KEY", "PROD_KEY"])
df_low_stock_impact = df_low_stock_impact.select(
    df_low_stock_impact["YR_WK_NUM"],
    df_low_stock_impact["STORE_KEY"],
    df_low_stock_impact["PROD_KEY"],
    (df_low_stock_impact["out_of_stock_count"] + df_low_stock_impact["low_stock_count"]).alias("total_low_stock_impact")
).orderBy("YR_WK_NUM", "STORE_KEY", "PROD_KEY")


# df_low_stock_impact.show()

                               
# calculate potential Low Stock Impact: if Low_Stock_Flg = 1 then SUM(SALES_QTY - INVENTORY_ON_HAND_QTY) for each week, each store, each product
# join df_low_stock to use low_stock_flg  correctly
# choose YR_WK_NUM from merged_df

df_potential_low_stock_impact = merged_df.join(df_low_stock, ["YR_WK_NUM", "STORE_KEY", "PROD_KEY"], "inner") \
    .filter(col("low_stock_flg") == 1) \
    .groupBy("YR_WK_NUM", "STORE_KEY", "PROD_KEY") \
    .agg(sum(col("SALES_QTY") - col("INVENTORY_ON_HAND_QTY")).alias("potential_low_stock_impact")) \
    .orderBy("YR_WK_NUM", "STORE_KEY", "PROD_KEY")

# df_potential_low_stock_impact.show()

# calculate no Stock Instances: Calculate then how many times of out_of_Stock_Flg in a week for each store, each product
# join df_low_stock to use out_of_stock_flg correctly
 
df_no_stock_instances = merged_df.join(df_low_stock, ["YR_WK_NUM", "STORE_KEY", "PROD_KEY"], "inner") \
   .filter(col("out_of_stock_flg") == 1) \
  .groupBy("YR_WK_NUM", "STORE_KEY", "PROD_KEY") \
  .agg(count("out_of_stock_flg").alias("no_stock_instances")) \
 .orderBy("YR_WK_NUM", "STORE_KEY", "PROD_KEY")
                                  
                                  
# df_no_stock_instances.show()


# calculate how many weeks the on hand stock can supply: 
# (inventory_on_hand_qty at the end of the week) / sum(sales_qty) for each week, each store, each product

df_weeks_supply = merged_df.groupBy("YR_WK_NUM", "STORE_KEY", "PROD_KEY", "INVENTORY_ON_HAND_QTY") \
    .agg((col("INVENTORY_ON_HAND_QTY") / sum(col("sales_qty"))).alias("weeks_supply")) \
    .orderBy("YR_WK_NUM", "STORE_KEY", "PROD_KEY")
                      
                            
# df_weeks_supply.show()

# add the calculated values from each dataframe into merged_df using YR_WK_NUM, PROD_KEY, STORE_KEY as the keys
# join df_sum_sales_amt_qty_cost to merged_df

df_final = df_sum_sales_amt_qty_cost.alias("df_sum") \
    .join(df_stock_level.alias("df_stock_level"), ["YR_WK_NUM","STORE_KEY", "PROD_KEY"]) \
    .join(df_stock_on_order.alias("df_stock_order"), ["YR_WK_NUM","STORE_KEY", "PROD_KEY"]) \
    .join(df_sum_cost.alias("df_sum_cost"), ["YR_WK_NUM","STORE_KEY", "PROD_KEY"]) \
    .join(df_out_of_stock.alias("df_out_of_stock"), ["YR_WK_NUM","STORE_KEY", "PROD_KEY"]) \
    .join(df_low_stock_impact.alias("df_low_stock_impact"), ["YR_WK_NUM","STORE_KEY", "PROD_KEY"]) \
    .join(df_potential_low_stock_impact.alias("df_potential_low_stock_impact"), ["YR_WK_NUM","STORE_KEY", "PROD_KEY"]) \
    .join(df_no_stock_instances.alias("df_no_stock_instances"), ["YR_WK_NUM","STORE_KEY", "PROD_KEY"]) \
    .join(df_weeks_supply.alias("df_weeks_supply"), ["YR_WK_NUM","STORE_KEY", "PROD_KEY"]) \
    .select(
        "df_sum.*",
        "df_stock_level.INVENTORY_ON_HAND_QTY_END_WEEK",
        "df_stock_order.INVENTORY_ON_ORDER_QTY_END_WEEK",
        "df_sum_cost.total_cost_week",
        "df_out_of_stock.out_of_stock_count",
        "df_low_stock_impact.total_low_stock_impact",
        "df_potential_low_stock_impact.potential_low_stock_impact",
        "df_no_stock_instances.no_stock_instances",
        "df_weeks_supply.weeks_supply"
    )

# write the final dataframe to parquet file and save it in the output folder in the S3 bucket
(
    df_final.write
    .repartition(1)
    .mode("overwrite")
    .option("header", "true")
    .option("compression","gzip")    
    # .parquet(f"file:////home/ubuntu/midterm/data/result/combined/date={date_str}")
    .parquet(f"s3a://wcd-midterm-s3/output/sales/date={date_str}")
    # .csv(f"file:////home/ubuntu/midterm/data/result/date={date_str}")
)

# copy the store, product and calendar files to the output folder
(
    store_df.write
    .repartition(1)
    .mode("overwrite")
    .option("header", "true")
    .option("compression","gzip")
    # .parquet(f"file:////home/ubuntu/midterm/data/result/store/date={date_str}")
    .parquet(f"s3a://wcd-midterm-s3/output/store/date={date_str}")
    # .csv(f"file:////home/ubuntu/midterm/data/result/store/date={date_str}")

)

(
    product_df.write
    .repartition(1)
    .mode("overwrite")
    .option("header", "true")
    .option("compression","gzip")
    # .parquet(f"file:////home/ubuntu/midterm/data/result/product/date={date_str}")
    .parquet(f"s3a://wcd-midterm-s3/output/product/date={date_str}")
    # .csv(f"file:////home/ubuntu/midterm/data/result/product/date={date_str}")
)

(
    calendar_df.write
    .repartition(1)
    .mode("overwrite")
    .option("header", "true")
    .option("compression","gzip")
    # .parquet(f"file:////home/ubuntu/midterm/data/result/calendar/date={date_str}")
    .parquet(f"s3a://wcd-midterm-s3/output/calendar/date={date_str}")
    # .csv(f"file:////home/ubuntu/midterm/data/result/calendar/date={date_str}")
)
