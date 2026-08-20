import sqlite3

import pandas as pd
from prefect import flow, task
from prefect.logging import get_run_logger


SOURCE_DB = "shopdata.db"


# ETL-01: Extract Customers
@task
def extract_customers() -> pd.DataFrame:
    logger = get_run_logger()

    try:
        logger.info("Extracting customer data from vw_raw_customers")

        connection = sqlite3.connect(
            f"file:{SOURCE_DB}?mode=ro",
            uri=True
        )

        query = """
        SELECT
            customer_id,
            full_name,
            email,
            phone,
            signup_date
        FROM vw_raw_customers;
        """

        customers_df = pd.read_sql_query(query, connection)

        logger.info(
            f"Extracted {len(customers_df)} customer records"
        )

        return customers_df

    except Exception as error:
        logger.error(f"Failed to extract customer data: {error}")
        raise

    finally:
        if "connection" in locals():
            connection.close()


# ETL-02: Extract Orders
@task
def extract_orders() -> pd.DataFrame:
    logger = get_run_logger()

    try:
        logger.info("Extracting order data from vw_raw_orders")

        connection = sqlite3.connect(
            f"file:{SOURCE_DB}?mode=ro",
            uri=True
        )

        query = """
        SELECT
            order_id,
            customer_id,
            order_date,
            total_amount,
            currency,
            status
        FROM vw_raw_orders;
        """

        orders_df = pd.read_sql_query(query, connection)

        logger.info(
            f"Extracted {len(orders_df)} order records"
        )

        return orders_df

    except Exception as error:
        logger.error(f"Failed to extract order data: {error}")
        raise

    finally:
        if "connection" in locals():
            connection.close()


# ETL-03: Extract Exchange Rates
@task
def extract_exchange_rates() -> pd.DataFrame:
    logger = get_run_logger()

    try:
        logger.info(
            "Extracting exchange rate data from vw_exchange_rates"
        )

        connection = sqlite3.connect(
            f"file:{SOURCE_DB}?mode=ro",
            uri=True
        )

        query = """
        SELECT
            currency,
            rate_to_usd,
            date
        FROM vw_exchange_rates;
        """

        exchange_rates_df = pd.read_sql_query(
            query,
            connection
        )

        logger.info(
            f"Extracted {len(exchange_rates_df)} exchange rate records"
        )

        return exchange_rates_df

    except Exception as error:
        logger.error(
            f"Failed to extract exchange rate data: {error}"
        )
        raise

    finally:
        if "connection" in locals():
            connection.close()


@flow(name="shopdata-etl-pipeline")
def etl_pipeline():
    customers_df = extract_customers()
    orders_df = extract_orders()
    exchange_rates_df = extract_exchange_rates()

    print(f"Customer rows: {len(customers_df)}")
    print(f"Order rows: {len(orders_df)}")
    print(f"Exchange rate rows: {len(exchange_rates_df)}")


if __name__ == "__main__":
    etl_pipeline()
