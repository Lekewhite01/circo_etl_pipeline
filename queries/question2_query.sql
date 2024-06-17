-- Question - Which store has the highest average sales amount per month?
WITH monthly_sales AS (
        SELECT 
            store_name, 
            sale_month, 
            SUM(total_amount) AS monthly_sales_amount
        FROM 
            sales_product_tbl
        GROUP BY 
            sale_month,
            store_name 
    )
    SELECT 
        store_name, 
        AVG(monthly_sales_amount) AS avg_sales_amount_per_month
    FROM 
        monthly_sales
    GROUP BY 
        store_name
    ORDER BY 
        avg_sales_amount_per_month DESC
    LIMIT 1