# Metrics Description for EMR_PySpark Project

This document describes the metrics computed by the `EMR_pyspark.py` script. The script performs several complex data manipulations and aggregations on a collection of dataframes representing calendar, inventory, product, sales, and store information.

## Metrics Overview

The main metrics computed by the script are:

- Total Sales Amount
- Total Sales Quantity
- Average Sales Price
- Total Cost of the Week
- Stock Level by the End of the Week
- Stock On Order Level by the End of the Week
- Percentage of Store In-Stock
- Low Stock Flag Instances
- Total Low Stock Impact
- Potential Low Stock Impact
- No Stock Instances
- Weeks of Supply

## Metric Details

### 1. Total Sales Amount
Computed by summing the sales amount for each week, store, and product.

### 2. Total Sales Quantity
Computed by summing the sales quantity for each week, store, and product.

### 3. Average Sales Price
Calculated by taking the average of sales amount for each week, store, and product.

### 4. Total Cost of the Week
Calculated by summing the sales cost for each week, store, and product.

### 5. Stock Level by the End of the Week
Represents the stock level on the last day of the week for each week, store, and product.

### 6. Stock On Order Level by the End of the Week
Represents the stock on order level on the last day of the week for each week, store, and product.

### 7. Percentage of Store In-Stock
Calculated by summing up the `out_of_stock_flg` for each week, store, and product and dividing it by 7.

### 8. Low Stock Flag Instances
A flag that's set if today's stock on hand quantity is less than the sales quantity for each day, week, store, and product.

### 9. Total Low Stock Impact
Computed as the sum of `out_of_stock_flg` and `low_stock_flg` for each week, store, and product.

### 10. Potential Low Stock Impact
Sum of the difference between sales quantity and inventory on hand when `low_stock_flg` is set to 1.

### 11. No Stock Instances
Count of instances where `out_of_stock_flg` is set to 1.

### 12. Weeks of Supply
Calculated as (inventory_on_hand_qty at the end of the week) / sum(sales_qty) for each week, store, and product.

## Output Format

The final results are written as Parquet files to an S3 bucket, segmented by date. Separate Parquet files for store, product, and calendar information are also written to the same S3 bucket.
