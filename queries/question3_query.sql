-- Question - What is the total sales amount and quantity sold for each product category in the past year?
SELECT product_category, SUM(total_amount) as total_sales_amount, SUM(quantity) as total_sales_quantity
        FROM sales_product_tbl
        GROUP BY product_category