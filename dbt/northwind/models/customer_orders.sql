with customers as (
    SELECT
        customer_id,
        contact_name as "name"
    FROM icerest.ingestion.cdc_northwind_public_customers
),

orders as (
    SELECT 
        order_id,
        customer_id,
        date_add('day', order_date, date '1970-01-01') AS order_date
        
    FROM icerest.ingestion.cdc_northwind_public_orders
),

customer_orders AS (
    SELECT
        customer_id,
        min(order_date) AS first_order_date,
        max(order_date) AS most_recent_order_date,
        count(order_id) AS number_of_orders
    FROM
        orders
    GROUP BY
        1
),

final AS (
    SELECT
        customers.customer_id,
        customers.name,
        customer_orders.first_order_date,
        customer_orders.most_recent_order_date,
        coalesce(customer_orders.number_of_orders, 0) AS number_of_orders
    FROM customers LEFT JOIN customer_orders ON customers.customer_id = customer_orders.customer_id
)
SELECT * FROM final