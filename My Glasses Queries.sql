create database myglass;
use myglass;

-- DIM_DATE
CREATE TABLE dim_date (
    date_id DATE PRIMARY KEY,
    day INT,
    month INT,
    quarter INT,
    year INT
);

-- DIM_CUSTOMER
CREATE TABLE dim_customer (
    customer_id INT PRIMARY KEY,
    age INT,
    gender VARCHAR(10),
    location VARCHAR(50),
    loyalty_tier VARCHAR(20)
);

-- DIM_STORE
CREATE TABLE dim_store (
    store_id INT PRIMARY KEY,
    city VARCHAR(50),
    region VARCHAR(50),
    type VARCHAR(50)
);

-- DIM_PRODUCT
CREATE TABLE dim_product (
    product_id INT PRIMARY KEY,
    sku VARCHAR(50),
    name VARCHAR(100),
    brand VARCHAR(50),
    type VARCHAR(50),
    price DECIMAL(10, 2)
);

-- SALES_FACT
CREATE TABLE sales_fact (
    sale_id INT PRIMARY KEY,
    store_id INT,
    product_id INT,
    customer_id INT,
    date_id DATE,
    quantity INT,
    discount_applied DECIMAL(10, 2),
    revenue DECIMAL(10, 2),
    FOREIGN KEY (store_id) REFERENCES dim_store(store_id),
    FOREIGN KEY (product_id) REFERENCES dim_product(product_id),
    FOREIGN KEY (customer_id) REFERENCES dim_customer(customer_id),
    FOREIGN KEY (date_id) REFERENCES dim_date(date_id)
);

-- RETURN_FACT
CREATE TABLE return_fact (
    return_id INT PRIMARY KEY AUTO_INCREMENT,
    sale_id INT,
    product_id INT,
    customer_id INT,
    store_id INT,
    return_date DATE,
    reason VARCHAR(100),
    refund_amount DECIMAL(10, 2),
    FOREIGN KEY (sale_id) REFERENCES sales_fact(sale_id),
    FOREIGN KEY (product_id) REFERENCES dim_product(product_id),
    FOREIGN KEY (customer_id) REFERENCES dim_customer(customer_id),
    FOREIGN KEY (store_id) REFERENCES dim_store(store_id),
    FOREIGN KEY (return_date) REFERENCES dim_date(date_id)
);

-- INVENTORY_FACT
CREATE TABLE inventory_fact (
    inventory_id INT PRIMARY KEY AUTO_INCREMENT,
    store_id INT,
    product_id INT,
    stock_on_hand INT,
    reorder_level INT,
    last_restocked DATE,
    FOREIGN KEY (store_id) REFERENCES dim_store(store_id),
    FOREIGN KEY (product_id) REFERENCES dim_product(product_id),
    FOREIGN KEY (last_restocked) REFERENCES dim_date(date_id)
);


select * from dim_date;
select * from dim_customer;
select * from dim_store;
select * from dim_product;
select * from sales_fact;
select * from return_fact;
select * from inventory_fact;
select * from dim_campaign;
select * from store_expense_fact;

-- 1. Which products are generating the highest revenue overall and by region?
SELECT
    region,
    product_name,
    total_revenue
FROM (
    SELECT 
        ds.region,
        dp.name AS product_name,
        SUM(sf.revenue) AS total_revenue,
        ROW_NUMBER() OVER (PARTITION BY ds.region ORDER BY SUM(sf.revenue) DESC) AS rank_in_region
    FROM 
        sales_fact sf
    JOIN 
        dim_store ds ON sf.store_id = ds.store_id
    JOIN 
        dim_product dp ON sf.product_id = dp.product_id
    GROUP BY 
        ds.region, dp.name
) ranked_products
WHERE rank_in_region = 1
ORDER BY region;

-- 2. What are the top-selling eyewear types (frames, lenses, sunglasses)?
SELECT 
    dp.type AS Eyewear_Type,
    SUM(sf.revenue) AS Total_Revenue
FROM 
    sales_fact sf
JOIN 
    dim_product dp ON sf.product_id = dp.product_id
GROUP BY 
    dp.type
ORDER BY 
    Total_Revenue DESC;

-- 3. Which stores consistently outperform others in terms of monthly sales?
SELECT 
    ds.store_id,
    ds.city,
    ds.region,
    ROUND(AVG(monthly_revenue), 2) AS avg_monthly_revenue
FROM (
    SELECT 
        sf.store_id,
        dd.year,
        dd.month,
        SUM(sf.revenue) AS monthly_revenue
    FROM 
        sales_fact sf
    JOIN 
        dim_date dd ON sf.date_id = dd.date_id
    GROUP BY 
        sf.store_id, dd.year, dd.month
) store_monthly
JOIN dim_store ds ON store_monthly.store_id = ds.store_id
GROUP BY 
    ds.store_id, ds.city, ds.region
ORDER BY 
    avg_monthly_revenue DESC;
    
-- 4. What is the sales growth trend over time (daily, monthly, quarterly)?
SELECT 
    dd.date_id,
    SUM(sf.revenue) AS daily_revenue
FROM 
    sales_fact sf
JOIN 
    dim_date dd ON sf.date_id = dd.date_id
GROUP BY 
    dd.date_id
ORDER BY 
    dd.date_id;
    
-- 5. What percentage of revenue comes from repeat customers vs new customers?
WITH customer_purchase_counts AS (
    SELECT 
        customer_id,
        COUNT(*) AS total_purchases
    FROM sales_fact
    GROUP BY customer_id
),
customer_revenue AS (
    SELECT 
        sf.customer_id,
        SUM(sf.revenue) AS total_revenue
    FROM sales_fact sf
    GROUP BY sf.customer_id
),
customer_type AS (
    SELECT 
        cr.customer_id,
        cr.total_revenue,
        CASE 
            WHEN cpc.total_purchases > 1 THEN 'Repeat'
            ELSE 'New'
        END AS customer_type
    FROM customer_revenue cr
    JOIN customer_purchase_counts cpc ON cr.customer_id = cpc.customer_id
)
SELECT
    customer_type,
    SUM(total_revenue) AS total_revenue,
    ROUND(
        100 * SUM(total_revenue) / (SELECT SUM(total_revenue) FROM customer_type), 2
    ) AS percentage_of_total
FROM customer_type
GROUP BY customer_type;

-- 6. What are the peak sales days of the week or month?
SELECT 
    DAYOFWEEK(sf.date_id) AS day_of_week_num,
    CASE DAYOFWEEK(sf.date_id)
        WHEN 1 THEN 'Sunday'
        WHEN 2 THEN 'Monday'
        WHEN 3 THEN 'Tuesday'
        WHEN 4 THEN 'Wednesday'
        WHEN 5 THEN 'Thursday'
        WHEN 6 THEN 'Friday'
        WHEN 7 THEN 'Saturday'
    END AS day_of_week,
    COUNT(sf.sale_id) AS total_sales_count,
    SUM(sf.revenue) AS total_revenue
FROM sales_fact sf
JOIN dim_date dd ON sf.date_id = dd.date_id
GROUP BY day_of_week_num, day_of_week
ORDER BY total_revenue DESC;

SELECT 
    DAY(sf.date_id) AS day_of_month,
    COUNT(sf.sale_id) AS total_sales_count,
    SUM(sf.revenue) AS total_revenue
FROM sales_fact sf
JOIN dim_date dd ON sf.date_id = dd.date_id
GROUP BY day_of_month
ORDER BY total_revenue DESC;

-- 7. What is the average transaction value per customer across stores?
SELECT
    sf.store_id,
    COUNT(sf.sale_id) AS total_transactions,
    SUM(sf.revenue) AS total_revenue,
    ROUND(SUM(sf.revenue) / COUNT(sf.sale_id), 2) AS avg_transaction_value
FROM sales_fact sf
GROUP BY sf.store_id
ORDER BY avg_transaction_value DESC;

-- 8. Which customer segments (age, loyalty tier) spend the most?
SELECT
    CASE
        WHEN dc.age BETWEEN 18 AND 25 THEN '18-25'
        WHEN dc.age BETWEEN 26 AND 35 THEN '26-35'
        WHEN dc.age BETWEEN 36 AND 45 THEN '36-45'
        WHEN dc.age BETWEEN 46 AND 55 THEN '46-55'
        WHEN dc.age >= 56 THEN '56+'
    END AS age_group,
    dc.loyalty_tier,
    COUNT(DISTINCT dc.customer_id) AS total_customers,
    SUM(sf.revenue) AS total_revenue,
    ROUND(SUM(sf.revenue) / COUNT(DISTINCT dc.customer_id), 2) AS avg_revenue_per_customer
FROM sales_fact sf
JOIN dim_customer dc ON sf.customer_id = dc.customer_id
GROUP BY age_group, dc.loyalty_tier
ORDER BY total_revenue DESC;

-- 9. How many units per product type are sold per region/store?
SELECT
    ds.region,
    ds.store_id,
    dp.type AS product_type,
    SUM(sf.quantity) AS total_units_sold
FROM sales_fact sf
JOIN dim_store ds ON sf.store_id = ds.store_id
JOIN dim_product dp ON sf.product_id = dp.product_id
GROUP BY ds.region, ds.store_id, dp.type
ORDER BY ds.region, ds.store_id, total_units_sold DESC;

-- 10. Which store-customer combinations generate the most business?
SELECT
    sf.store_id,
    sf.customer_id,
    COUNT(sf.sale_id) AS total_transactions,
    SUM(sf.revenue) AS total_revenue
FROM sales_fact sf
GROUP BY sf.store_id, sf.customer_id
ORDER BY total_revenue DESC
LIMIT 10;

-- 11. Which products are frequently low on stock and need restocking?
SELECT
    product_id,
    SUM(CASE WHEN stock_on_hand < reorder_level THEN 1 ELSE 0 END) AS low_stock_count,
    MIN(stock_on_hand) AS lowest_stock_seen,
    MAX(last_restocked) AS last_restocked_date
FROM inventory_fact
GROUP BY product_id
HAVING low_stock_count > 0
ORDER BY low_stock_count DESC, lowest_stock_seen ASC;

-- 12. What’s the average inventory turnover rate by store/product?
WITH sales_summary AS (
    SELECT
        store_id,
        product_id,
        SUM(quantity) AS total_quantity_sold
    FROM sales_fact
    GROUP BY store_id, product_id
),
inventory_summary AS (
    SELECT
        store_id,
        product_id,
        AVG(stock_on_hand) AS avg_stock_level
    FROM inventory_fact
    GROUP BY store_id, product_id
),
turnover_data AS (
    SELECT
        s.store_id,
        s.product_id,
        s.total_quantity_sold,
        i.avg_stock_level,
        CASE
            WHEN i.avg_stock_level = 0 THEN NULL
            ELSE s.total_quantity_sold / i.avg_stock_level
        END AS inventory_turnover_rate
    FROM sales_summary s
    JOIN inventory_summary i
      ON s.store_id = i.store_id AND s.product_id = i.product_id
)
SELECT
    store_id,
    product_id,
    ROUND(total_quantity_sold, 2) AS total_units_sold,
    ROUND(avg_stock_level, 2) AS average_inventory,
    ROUND(inventory_turnover_rate, 2) AS inventory_turnover_rate
FROM turnover_data
ORDER BY inventory_turnover_rate DESC;

-- 13. Which stores are overstocked or understocked across key SKUs?
WITH inventory_with_sku AS (
    SELECT
        i.store_id,
        p.sku,
        p.name AS product_name,
        i.product_id,
        i.stock_on_hand,
        i.reorder_level,
        AVG(i.stock_on_hand) OVER(PARTITION BY i.product_id) AS avg_stock_level
    FROM inventory_fact i
    JOIN dim_product p ON i.product_id = p.product_id
),
stock_status AS (
    SELECT
        store_id,
        sku,
        product_name,
        stock_on_hand,
        reorder_level,
        avg_stock_level,
        CASE
            WHEN stock_on_hand < reorder_level THEN 'Understocked'
            WHEN stock_on_hand > 2 * avg_stock_level THEN 'Overstocked'
            ELSE 'Normal Stock'
        END AS stock_status
    FROM inventory_with_sku
)
SELECT
    store_id,
    sku,
    product_name,
    stock_on_hand,
    reorder_level,
    avg_stock_level,
    stock_status
FROM stock_status
WHERE stock_status IN ('Understocked', 'Overstocked')
ORDER BY stock_status DESC, store_id, sku;

-- 14. How often are restocks happening, and are they timely?
 WITH restock_stats AS (
    SELECT
        product_id,
        store_id,
        COUNT(*) AS restock_count,
        MIN(last_restocked) AS first_restock_date,
        MAX(last_restocked) AS latest_restock_date,
        DATEDIFF(MAX(last_restocked), MIN(last_restocked)) AS total_days,
        ROUND(DATEDIFF(MAX(last_restocked), MIN(last_restocked)) / COUNT(*), 2) AS avg_days_between_restocks
    FROM inventory_fact
    GROUP BY product_id, store_id
),
restock_timeliness AS (
    SELECT
        i1.product_id,
        i1.store_id,
        SUM(CASE WHEN i1.stock_on_hand <= i1.reorder_level THEN 1 ELSE 0 END) AS timely_restocks,
        SUM(CASE WHEN i1.stock_on_hand > i1.reorder_level THEN 1 ELSE 0 END) AS early_restocks,
        SUM(CASE 
            WHEN (
                SELECT i2.stock_on_hand
                FROM inventory_fact i2
                WHERE i2.product_id = i1.product_id
                  AND i2.store_id = i1.store_id
                  AND i2.last_restocked > i1.last_restocked
                ORDER BY i2.last_restocked ASC
                LIMIT 1
            ) = 0 THEN 1 ELSE 0 END
        ) AS late_restocks
    FROM inventory_fact i1
    GROUP BY i1.product_id, i1.store_id
)
SELECT
    rs.product_id,
    rs.store_id,
    rs.restock_count,
    rs.avg_days_between_restocks,
    rt.timely_restocks,
    rt.early_restocks,
    rt.late_restocks,
    ROUND((rt.timely_restocks + rt.early_restocks) / rs.restock_count * 100, 2) AS percent_on_time_restocks
FROM restock_stats rs
JOIN restock_timeliness rt ON rs.product_id = rt.product_id AND rs.store_id = rt.store_id
ORDER BY percent_on_time_restocks DESC;

-- 15. What is the lead time between restock and product availability?
SELECT
    i.store_id,
    i.product_id,
    i.last_restocked AS restock_date,
    MIN(s.date_id) AS first_sale_after_restock,
    DATEDIFF(MIN(s.date_id), i.last_restocked) AS days_to_availability
FROM inventory_fact i
JOIN sales_fact s
  ON i.store_id = s.store_id
  AND i.product_id = s.product_id
  AND s.date_id >= i.last_restocked
GROUP BY i.store_id, i.product_id, i.last_restocked
ORDER BY days_to_availability DESC;

-- 16. How effective is the reorder level setting per product category?
WITH simulated_product_categories AS (
    SELECT 
        product_id,
        CASE 
            WHEN product_id BETWEEN 1000 AND 1030 THEN 'Frame'
            WHEN product_id BETWEEN 1031 AND 1060 THEN 'Lens'
            WHEN product_id BETWEEN 1061 AND 1090 THEN 'Sunglasses'
            ELSE 'Other'
        END AS product_category
    FROM inventory_fact
    GROUP BY product_id
),
inventory_with_category AS (
    SELECT
        i.*,
        spc.product_category
    FROM inventory_fact i
    JOIN simulated_product_categories spc ON i.product_id = spc.product_id
),
stockout_analysis AS (
    SELECT
        product_category,
        COUNT(*) AS total_inventory_records,
        SUM(CASE WHEN stock_on_hand = 0 THEN 1 ELSE 0 END) AS times_stock_ran_out,
        ROUND(AVG(reorder_level), 2) AS avg_reorder_level,
        ROUND(AVG(stock_on_hand), 2) AS avg_stock_on_hand
    FROM inventory_with_category
    GROUP BY product_category
)
SELECT
    product_category,
    total_inventory_records,
    times_stock_ran_out,
    avg_reorder_level,
    avg_stock_on_hand,
    ROUND((1 - (times_stock_ran_out / total_inventory_records)) * 100, 2) AS effectiveness_score_percent
FROM stockout_analysis
ORDER BY effectiveness_score_percent DESC;

-- 17. What is the return rate by product type, brand, or store?
SELECT
    p.type AS product_type,
    p.brand,
    st.region,
    COUNT(s.sale_id) AS total_sales,
    COUNT(r.return_id) AS total_returns,
    ROUND((COUNT(r.return_id) / COUNT(s.sale_id)) * 100, 2) AS return_rate_percent
FROM sales_fact s
JOIN dim_product p ON s.product_id = p.product_id
JOIN dim_store st ON s.store_id = st.store_id
LEFT JOIN return_fact r ON s.sale_id = r.sale_id
GROUP BY p.type, p.brand, st.region
ORDER BY return_rate_percent DESC;

-- 18. What are the top reasons for returns, and how can we reduce them?
SELECT
    reason,
    COUNT(*) AS total_returns,
    SUM(refund_amount) AS total_refund_value,
    ROUND(AVG(refund_amount), 2) AS avg_refund_per_return
FROM return_fact
GROUP BY reason
ORDER BY total_returns DESC;

-- 19. What is the financial impact (in INR) of returns by category and location?
SELECT
    CASE
        WHEN r.product_id BETWEEN 1000 AND 1030 THEN 'Frame'
        WHEN r.product_id BETWEEN 1031 AND 1060 THEN 'Lens'
        WHEN r.product_id BETWEEN 1061 AND 1090 THEN 'Sunglasses'
        ELSE 'Other'
    END AS product_category,
    st.region,
    COUNT(r.return_id) AS total_returns,
    SUM(r.refund_amount) AS total_refunds,
    ROUND(AVG(r.refund_amount), 2) AS avg_refund_per_return
FROM return_fact r
JOIN dim_store st ON r.store_id = st.store_id
GROUP BY product_category, st.region
ORDER BY total_refunds DESC;

-- 20. Are certain stores experiencing higher return rates than others?
SELECT
    st.store_id,
    st.city,
    st.region,
    COUNT(s.sale_id) AS total_sales,
    COUNT(r.return_id) AS total_returns,
    ROUND((COUNT(r.return_id) / COUNT(s.sale_id)) * 100, 2) AS return_rate_percent,
    ROUND(SUM(r.refund_amount), 2) AS total_refunds_inr
FROM sales_fact s
JOIN dim_store st ON s.store_id = st.store_id
LEFT JOIN return_fact r ON s.sale_id = r.sale_id
GROUP BY st.store_id, st.city, st.region
ORDER BY return_rate_percent DESC;

-- 21. What is the average time between purchase and return?
SELECT 
    ROUND(AVG(DATEDIFF(r.return_date, s.date_id)), 2) AS avg_days_between_purchase_and_return
FROM 
    return_fact r
JOIN 
    sales_fact s 
    ON r.sale_id = s.sale_id;
-- 22. Which customer segments are more likely to return products?
SELECT
    c.loyalty_tier,
    COUNT(s.sale_id) AS total_purchases,
    COUNT(r.return_id) AS total_returns,
    ROUND((COUNT(r.return_id) / COUNT(s.sale_id)) * 100, 2) AS return_rate_percent
FROM sales_fact s
JOIN dim_customer c ON s.customer_id = c.customer_id
LEFT JOIN return_fact r ON s.sale_id = r.sale_id
GROUP BY c.loyalty_tier
ORDER BY return_rate_percent DESC;

-- 23. What is the average customer lifetime value (CLV)?
SELECT
    COUNT(DISTINCT customer_id) AS total_customers,
    SUM(revenue) AS total_revenue,
    ROUND(SUM(revenue) / COUNT(DISTINCT customer_id), 2) AS avg_clv
FROM sales_fact;



-- 24. Which loyalty tier has the highest engagement and revenue contribution?
SELECT
    dc.loyalty_tier,
    COUNT(sf.sale_id) AS total_purchases,
    COUNT(r.return_id) AS total_returns,
    ROUND((COUNT(r.return_id) / COUNT(sf.sale_id)) * 100, 2) AS return_rate_percent,
    SUM(sf.revenue) AS total_revenue
FROM sales_fact sf
JOIN dim_customer dc ON sf.customer_id = dc.customer_id
LEFT JOIN return_fact r ON sf.sale_id = r.sale_id
GROUP BY dc.loyalty_tier
ORDER BY return_rate_percent DESC;

-- 25. What are the most popular products among different age groups?
WITH ranked_sales AS (
    SELECT
        CASE
            WHEN c.age BETWEEN 18 AND 25 THEN '18-25'
            WHEN c.age BETWEEN 26 AND 35 THEN '26-35'
            WHEN c.age BETWEEN 36 AND 45 THEN '36-45'
            WHEN c.age BETWEEN 46 AND 55 THEN '46-55'
            ELSE '56+'
        END AS age_group,
        s.product_id,
        COUNT(*) AS total_purchases,
        RANK() OVER(PARTITION BY 
                    CASE
                        WHEN c.age BETWEEN 18 AND 25 THEN '18-25'
                        WHEN c.age BETWEEN 26 AND 35 THEN '26-35'
                        WHEN c.age BETWEEN 36 AND 45 THEN '36-45'
                        WHEN c.age BETWEEN 46 AND 55 THEN '46-55'
                        ELSE '56+'
                    END 
                    ORDER BY COUNT(*) DESC) AS product_rank
    FROM sales_fact s
    JOIN dim_customer c ON s.customer_id = c.customer_id
    GROUP BY age_group, s.product_id
)
SELECT *
FROM ranked_sales
WHERE product_rank = 1
ORDER BY age_group;

-- 26. How do customer preferences vary by geography?
SELECT
    c.location,
    p.type AS product_type,
    COUNT(*) AS total_purchases,
    COUNT(DISTINCT c.customer_id) AS unique_customers,
    SUM(s.revenue) AS total_revenue,
    ROUND(AVG(s.revenue), 2) AS avg_order_value
FROM sales_fact s
JOIN dim_customer c ON s.customer_id = c.customer_id
JOIN dim_product p ON s.product_id = p.product_id
GROUP BY c.location, p.type
ORDER BY c.location, total_revenue DESC;

-- 28. What are the upsell and cross-sell opportunities based on past purchases?
WITH PopularProducts AS (
    SELECT 
        product_id, 
        COUNT(*) AS purchase_count
    FROM 
        sales_fact
    GROUP BY 
        product_id
    ORDER BY 
        purchase_count DESC
    LIMIT 10
),
UpsellCandidates AS (
    SELECT 
        pp.product_id AS original_product_id,
        dp.name AS original_product_name,
        dp.price AS original_price,
        dp2.product_id AS upsell_product_id,
        dp2.name AS upsell_product_name,
        dp2.price AS upsell_price
    FROM 
        PopularProducts pp
    JOIN 
        dim_product dp ON pp.product_id = dp.product_id
    JOIN 
        dim_product dp2 ON dp.type = dp2.type 
                    AND dp2.price > dp.price
)
SELECT DISTINCT 
    original_product_name,
    original_price,
    upsell_product_name,
    upsell_price
FROM 
    UpsellCandidates
ORDER BY 
    original_product_name, upsell_price DESC;
    
-- 29. How many first-time customers convert into repeat buyers within 90 days?
WITH FirstPurchase AS (
    -- Get the first purchase date for each customer
    SELECT 
        customer_id,
        MIN(date_id) AS first_purchase_date
    FROM 
        sales_fact
    GROUP BY 
        customer_id
),
RepeatWithin90Days AS (
    -- Check if customer has another purchase within 90 days of first purchase
    SELECT 
        fp.customer_id
    FROM 
        FirstPurchase fp
    JOIN 
        sales_fact s 
        ON fp.customer_id = s.customer_id
       AND s.date_id > fp.first_purchase_date
       AND s.date_id <= DATE_ADD(fp.first_purchase_date, INTERVAL 90 DAY)
    GROUP BY 
        fp.customer_id
    HAVING 
        COUNT(s.sale_id) >= 1  -- At least one repeat purchase
)
-- Count total number of such customers
SELECT 
    COUNT(*) AS repeat_customers_count
FROM 
    RepeatWithin90Days;
    
-- 30. Which city or region should MyGlasses expand into next based on current demand patterns?
WITH CustomerLocation AS (
    SELECT 
        dc.location AS city,
        COUNT(DISTINCT dc.customer_id) AS total_customers
    FROM 
        dim_customer dc
    GROUP BY 
        dc.location
),
SalesByCity AS (
    SELECT 
        ds.city,
        COUNT(sf.sale_id) AS total_sales,
        SUM(sf.revenue) AS total_revenue,
        COUNT(DISTINCT sf.customer_id) AS unique_buyers
    FROM 
        sales_fact sf
    JOIN 
        dim_store ds ON sf.store_id = ds.store_id
    GROUP BY 
        ds.city
),
CombinedMetrics AS (
    SELECT 
        cl.city,
        cl.total_customers,
        COALESCE(sb.total_sales, 0) AS total_sales,
        COALESCE(sb.total_revenue, 0) AS total_revenue,
        COALESCE(sb.unique_buyers, 0) AS unique_buyers,
        CASE 
            WHEN cl.total_customers > 0 THEN COALESCE(sb.total_sales, 0) / cl.total_customers 
            ELSE 0 
        END AS sales_per_customer
    FROM 
        CustomerLocation cl
    LEFT JOIN 
        SalesByCity sb ON cl.city = sb.city
)
SELECT 
    city,
    total_customers,
    total_sales,
    total_revenue,
    unique_buyers,
    sales_per_customer
FROM 
    CombinedMetrics
ORDER BY 
    sales_per_customer ASC  -- Prioritize cities with low conversion rate
LIMIT 10;
