# ETL Pipeline

## Setup Instructions

1. **Install Dependencies**:
    - Ensure you have Python (at least 3.9) and Apache Spark installed. Install necessary Python packages using:
        ```bash
        pip install pyspark pandas sqlite3
        ```

2. **Enter the pipeline directory**:
    - The `sales.csv`, `products.csv`, and `stores.csv` files are in the same folder as the ETL script which is `pipeline`.
    - Use this command to access the folder from the root:
        ```bash
        cd pipeline
        ```

3. **Run the Pipeline**:
    - Once inside the pipeline folder, execute the Python script:
        ```bash
        python etl_pipeline.py
        ```

## SQL Queries

- To access the SQL queries, enter the queries folder from the root:
```bash
cd queries
```
## Notes

- The script includes error handling to ensure no null values or duplicates are present before writing to the database.
- Indexes are created on relevant columns to optimize query performance.
- Apache spark 3.3.1 and pyspark 3.3.1 were used in this project. Its best to use at least these versions or higher.
