import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, year, month, count, when
from pyspark.sql.types import *
import sqlite3

# Start the timer to measure the pipeline runtime
start_time = time.time()

# Initialize Spark Session
spark = (
    SparkSession
    .builder
    .appName("product_sales_pipeline")
    .master("local[4]")  # Running locally on 4 cores
    .getOrCreate()
)

# Load data from CSV files
products = spark.read.option("header", True).csv("products.csv")
sales = spark.read.option("header", True).csv("sales.csv")
stores = spark.read.option("header", True).csv("stores.csv")

# Clean and cast products DataFrame
products = (
    products
    .na.replace("nan", None)
    .dropna()
    .dropDuplicates()
    .select(
        col("product_id").cast(IntegerType()), 
        col("product_name").cast(StringType()), 
        col("product_category").cast(StringType())
    )
)

# Clean and cast sales DataFrame
sales = (
    sales
    .na.replace("null", None)
    .dropna()
    .dropDuplicates()
    .select(
        col("sale_id").cast(StringType()), 
        col("product_id").cast(IntegerType()), 
        col("store_id").cast(IntegerType()), 
        col("sale_date").cast(DateType()), 
        col("quantity").cast(IntegerType()), 
        col("total_amount").cast(FloatType())
    )
)

# Clean and cast stores DataFrame
stores = (
    stores
    .na.replace("nan", None)
    .dropna()
    .dropDuplicates()
    .select(
        col("store_id").cast(IntegerType()), 
        col("store_name").cast(StringType()), 
        col("location").cast(StringType())
    )
)

# Extract month and year from sale_date and aggregate sales
sales = sales.withColumn("sale_month", month(col("sale_date"))).withColumn("sale_year", year(col("sale_date")))

# Group by product_id and sale_month to get total sales quantity and amount per month
aggregated_sales = sales.groupBy("sale_month", "product_id").agg(
    sum("quantity").alias("total_sales_quantity"),
    sum("total_amount").alias("total_sales_amount")
)
# Join the other sales columns in case they are required further down the pipeline
final_sales = sales.join(aggregated_sales, on=["product_id", "sale_month"], how="left")

# Join aggregated sales with products and stores DataFrames
sales_products_joined = final_sales.join(products, on="product_id", how="left")
final_df = sales_products_joined.join(stores, on="store_id", how="left")

# Select desired columns and clean final DataFrame
final_df = final_df.select(
    "product_id", "product_name", "product_category", 
    "store_id", "store_name", "location", 
    "sale_year", "sale_month", "quantity", "total_amount",
    "total_sales_quantity", "total_sales_amount"
)

final_df = final_df.dropna().dropDuplicates()

# Check for nulls in every column of the DataFrame
null_counts = final_df.select([count(when(col(c).isNull(), c)).alias(c) for c in final_df.columns])
null_counts.show()

# Check for duplicates
original_count = final_df.count()
distinct_count = final_df.distinct().count()

if original_count != distinct_count:
    print(f"There are duplicates in the DataFrame. Original count: {original_count}, Distinct count: {distinct_count}")
    data_valid = False
else:
    print("There are no duplicates in the DataFrame.")
    data_valid = True

# Check if there are any nulls
null_summary = null_counts.collect()[0].asDict()
if any(value > 0 for value in null_summary.values()):
    print("There are null values in the DataFrame:", null_summary)
    data_valid = False
else:
    print("There are no null values in the DataFrame.")
    data_valid = True

# Only write to the database if there are no nulls or duplicates
if data_valid:
    # Convert PySpark DataFrame to Pandas DataFrame for SQLite insertion
    final_df_pandas = final_df.toPandas()

    # Write the Pandas DataFrame to a SQLite database
    print("===== Writing DataFrame to SQLite DB =====")
    sqlite_db_path = "../database/sql_database.db"
    conn = sqlite3.connect(sqlite_db_path)
    final_df_pandas.to_sql("sales_product_tbl", conn, if_exists="replace", index=False)

    # Indexing for optimized SQL performance
    conn.execute("CREATE INDEX idx_product_id ON sales_product_tbl (product_id);")
    conn.execute("CREATE INDEX idx_store_id ON sales_product_tbl (store_id);")
    conn.execute("CREATE INDEX idx_sale_year_month ON sales_product_tbl (sale_year, sale_month);")
    conn.close()
    print("===== Writing to SQLite done! =====")

else:
    print("Data not written to SQLite due to null or duplicate values.")

# End the timer and calculate elapsed time
end_time = time.time()
elapsed_time = (end_time - start_time) / 60  # Convert to minutes
print(f"ETL pipeline took {elapsed_time:.2f} minutes to run.")
