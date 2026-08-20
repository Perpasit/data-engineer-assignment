# ShopData ETL Pipeline

Data Engineer Technical Assignment for building an ETL pipeline that extracts raw customer, order, and exchange-rate data from SQLite, applies data cleaning rules, and loads cleaned data into an analytics database for Customer Lifetime Value (CLV) reporting.

## Project Overview

The pipeline processes data from the following read-only SQLite views in `shopdata.db`:

- `vw_raw_customers`
- `vw_raw_orders`
- `vw_exchange_rates`

The cleaned data is loaded into `analytics.db` as:

- `dim_customers`
- `fct_orders`

The pipeline is orchestrated using Prefect and the transformation logic is tested using pytest.

## Project Structure

```text
data-engineer-assignment/
├── pipeline.py
├── exploration.sql
├── clv_report.sql
├── requirements.txt
├── README.md
├── shopdata.db
└── tests/
    └── test_pipeline.py
```

`analytics.db` is generated automatically when the pipeline runs and is not required to be committed to the repository.

## Requirements

- Python 3.12+
- Prefect 3.x
- pandas
- pytest
- SQLite

## Setup

### 1. Create a virtual environment

```bash
python -m venv .venv
```

### 2. Activate the virtual environment

Windows:

```bash
.venv\Scripts\activate
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Place the source database

Make sure the provided `shopdata.db` file is located in the project root directory.

```text
data-engineer-assignment/
├── shopdata.db
├── pipeline.py
└── ...
```

## Data Exploration

The source data was explored using the queries in `exploration.sql` before implementing the Python ETL pipeline.

Several data quality issues were identified:

1. **Duplicate customer records**  
   Some `customer_id` values appeared more than once with different signup dates. The pipeline keeps the record with the most recent `signup_date`.

2. **Missing customer information**  
   Some customer records contained missing email addresses and phone numbers. Missing emails are replaced with `unknown@domain.com`.

3. **Inconsistent phone number formats**  
   Phone values contained characters such as spaces, parentheses, hyphens, country-code symbols, and text. The pipeline removes all non-numeric characters.

4. **Invalid order amounts**  
   Some orders contained `total_amount` values less than or equal to zero. These records are treated as system errors and removed.

5. **Missing currencies or exchange rates**  
   Some orders have missing currencies or do not have a matching exchange rate for the order date. In these cases, the original amount is treated as already being in USD.

6. **Missing order dates**  
   At least one order contains a missing order date. Since no additional cleaning rule is defined for this case, the order is retained and falls back to the original amount when no exchange rate can be matched.

## ETL Pipeline

The pipeline follows three stages.

### Extract

Data is extracted from:

- `vw_raw_customers`
- `vw_raw_orders`
- `vw_exchange_rates`

### Transform

Customer transformations:

- Deduplicate customers using the most recent `signup_date`
- Remove non-numeric characters from phone numbers
- Replace missing emails with `unknown@domain.com`

Order transformations:

- Remove orders where `total_amount <= 0`
- Match exchange rates using `currency` and `order_date`
- Calculate `usd_amount`
- Treat the original amount as USD when currency or exchange-rate information is unavailable

### Load

The cleaned DataFrames are loaded into a new SQLite database:

```text
analytics.db
```

with two analytical tables:

```text
dim_customers
fct_orders
```

For the provided dataset, the validated pipeline result was:

```text
Raw customers:       12
Clean customers:     10

Raw orders:          20
Clean orders:        17

dim_customers:       10 rows
fct_orders:          17 rows
```

## Run the ETL Pipeline

From the project root with the virtual environment activated:

```bash
python pipeline.py
```

A successful run creates `analytics.db` and loads the cleaned tables.

Prefect logs will show each extraction, transformation, and loading task and the final flow state.

## Run Unit Tests

Run the tests with:

```bash
python -m pytest -v
```

The unit tests validate transformation logic independently from the SQLite databases.

Current tests cover:

- Phone number standardization
- Missing phone handling
- Invalid order filtering
- Currency conversion and exchange-rate fallback behavior

Validated test result:

```text
4 passed
```

## Customer Lifetime Value Report

After running the ETL pipeline, execute `clv_report.sql` against `analytics.db`.

The report returns:

- `customer_id`
- `full_name`
- `total_orders_placed`
- `lifetime_value_usd`
- `customer_cohort`

Results are ordered by `lifetime_value_usd` from highest to lowest.

Example query execution can be performed using DB Browser for SQLite or another SQLite client.

For the provided dataset, the CLV report returns 10 customers, including customers with no orders whose lifetime value is reported as `0`.

## Output

The complete process is:

```text
shopdata.db
     |
     v
Extract
     |
     v
Transform
     |
     +-------------------+
     |                   |
     v                   v
dim_customers       fct_orders
     |                   |
     +---------+---------+
               |
               v
          analytics.db
               |
               v
          CLV Report
```