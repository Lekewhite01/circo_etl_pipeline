-- Question - What are the top 5 products with the highest total sales amount in the past year?
SELECT 
        product_name,
        SUM(total_amount) AS total_sales_amount
    FROM 
        sales_product_tbl
    GROUP BY 
        product_name
    ORDER BY 
        total_sales_amount DESC
    LIMIT 5