import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format, sum
from pyspark.sql.types import *
import pandas as pd
import sqlite3

# Start the timer
start_time = time.time()

# Initialize Spark Session
spark = (
    SparkSession
    .builder
    .appName("circo_challenge")
    .master("local[4]")
    .getOrCreate()
)

# Load data
products = spark.read.option("header", True).csv("./products.csv")
sales = spark.read.option("header", True).csv("./sales.csv")
stores = spark.read.option("header", True).csv("./stores.csv")

# Clean and cast products DataFrame
products = products.replace("nan", None)
products = products.dropna()
products = products.dropDuplicates()
products = products.selectExpr(
    "cast(product_id as integer) as product_id",
    "cast(product_name as string) as product_name",
    "cast(product_category as string) as product_category"
)

# Clean and cast sales DataFrame
sales = sales.replace("null", None)
sales = sales.dropna()
sales = sales.dropDuplicates()
sales = sales.selectExpr(
    "cast(sale_id as string) as sale_id",
    "cast(product_id as integer) as product_id",
    "cast(store_id as integer) as store_id",
    "cast(sale_date as date) as sale_date",
    "cast(quantity as integer) as quantity",
    "cast(total_amount as float) as total_amount"
)

# Clean and cast stores DataFrame
stores = stores.replace("nan", None)
stores = stores.dropna()
stores = stores.dropDuplicates()
stores = stores.selectExpr(
    "cast(store_id as integer) as store_id",
    "cast(store_name as string) as store_name",
    "cast(location as string) as location"
)

# Extract month and year from sale_date and aggregate sales
sales = sales.withColumn("sale_month", date_format(col("sale_date"), "MM"))\
             .withColumn("sale_year", date_format(col("sale_date"), "yyyy"))

aggregated_sales = sales.groupBy("product_id", "sale_month") \
    .agg(
        sum("quantity").alias("total_monthly_quantity"),
        sum("total_amount").alias("total_monthly_amount")
    )

final_sales = sales.join(
    aggregated_sales,
    on=["product_id", "sale_month"],
    how="left"
).select("sale_id", "product_id", "store_id", "sale_date", "sale_month", "sale_year", "total_monthly_quantity", 
         "total_monthly_amount")

# Join aggregated sales with products and stores
sales_products_joined = final_sales.join(products, on="product_id", how="left")
final_df = sales_products_joined.join(stores, on="store_id", how="left")

# Select desired columns and clean final DataFrame
final_df = final_df.select(
    "product_id", "product_name", "product_category", 
    "store_id", "store_name", "location", "sale_id", "sale_date", "sale_month", "sale_year", 
    "total_monthly_quantity", "total_monthly_amount"
)

final_df = final_df.replace("nan", None)
final_df = final_df.dropna()
final_df = final_df.dropDuplicates()

# Convert PySpark DataFrame to Pandas DataFrame
final_df_pandas = final_df.toPandas()

# Write the Pandas DataFrame to a SQLite database
print("===== Writing DataFrame to SQLite DB =====")
sqlite_db_path = "sql_database.db"
conn = sqlite3.connect(sqlite_db_path)
final_df_pandas.to_sql("sales_product_tbl", conn, if_exists="replace", index=True)
conn.close()
print("===== Writing to SQLite done! =====")

# End the timer and calculate elapsed time
end_time = time.time()
elapsed_time = (end_time - start_time) / 60  # Convert to minutes

print(f"ETL pipeline took {elapsed_time:.2f} minutes to run.")
