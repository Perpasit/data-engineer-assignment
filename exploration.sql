-- DQ-01: Inspect source views and schema
SELECT name
FROM sqlite_master
WHERE type = 'view'
ORDER BY name;

PRAGMA table_info(vw_raw_customers);
PRAGMA table_info(vw_raw_orders);
PRAGMA table_info(vw_exchange_rates);

-- DQ-02: Count customer records
SELECT COUNT(*) AS total_customers
FROM vw_raw_customers;

-- DQ-03: Find duplicate customers
SELECT customer_id, COUNT(*) AS duplicate_count
FROM vw_raw_customers
GROUP BY customer_id
HAVING COUNT(*) > 1;

-- DQ-04: Find missing emails
SELECT *
FROM vw_raw_customers
WHERE email IS NULL
   OR TRIM(email) = '';

-- DQ-05: Find inconsistent phone formats
SELECT customer_id, phone
FROM vw_raw_customers
WHERE phone IS NULL
   OR phone GLOB '*[^0-9]*';

-- DQ-06: Count order records
SELECT COUNT(*) AS total_orders
FROM vw_raw_orders;

-- DQ-07: Find invalid order amounts
SELECT *
FROM vw_raw_orders
WHERE total_amount <= 0;

-- DQ-08: Check currency distribution
SELECT currency, COUNT(*) AS order_count
FROM vw_raw_orders
GROUP BY currency
ORDER BY order_count DESC;

-- DQ-09: Check exchange rate coverage
SELECT
    currency,
    COUNT(*) AS rate_count,
    MIN(date) AS first_date,
    MAX(date) AS last_date
FROM vw_exchange_rates
GROUP BY currency;

-- DQ-10: Find orders without matching exchange rates
SELECT
    o.order_id,
    o.order_date,
    o.currency,
    o.total_amount
FROM vw_raw_orders o
LEFT JOIN vw_exchange_rates r
    ON o.currency = r.currency
    AND o.order_date = r.date
WHERE o.currency IS NULL
   OR TRIM(o.currency) = ''
   OR r.rate_to_usd IS NULL;