{{
    config(
        tags=['some_tag']
    )
}}

monthly_orders as (
    SELECT 
        COUNT(order_id) as order_count,
        customer_id,
        DATEPART(MONTH, date_add('day', order_date, date '1970-01-01')) AS month
        
    FROM icerest.ingestion.cdc_northwind_public_orders
)


SELECT * FROM monthly_orders