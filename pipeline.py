import re
import sqlite3

import pandas as pd
from prefect import flow, task
from prefect.logging import get_run_logger


SOURCE_DB = "shopdata.db"


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


def deduplicate_customers(customers_df: pd.DataFrame) -> pd.DataFrame:
    cleaned_df = customers_df.copy()

    cleaned_df["signup_date"] = pd.to_datetime(
        cleaned_df["signup_date"]
    )

    cleaned_df = (
        cleaned_df
        .sort_values("signup_date")
        .drop_duplicates(
            subset="customer_id",
            keep="last"
        )
        .reset_index(drop=True)
    )

    return cleaned_df


def standardize_phone(phone):
    if pd.isna(phone):
        return None

    return re.sub(r"\D", "", str(phone))


def fill_missing_emails(customers_df: pd.DataFrame) -> pd.DataFrame:
    cleaned_df = customers_df.copy()

    cleaned_df["email"] = (
        cleaned_df["email"]
        .replace(r"^\s*$", pd.NA, regex=True)
        .fillna("unknown@domain.com")
    )

    return cleaned_df


@task
def transform_customers(
    customers_df: pd.DataFrame
) -> pd.DataFrame:

    logger = get_run_logger()

    try:
        before_count = len(customers_df)

        cleaned_df = deduplicate_customers(customers_df)

        after_dedup_count = len(cleaned_df)

        logger.info(
            f"Customer deduplication completed: "
            f"{before_count} -> {after_dedup_count} records"
        )

        cleaned_df["phone"] = cleaned_df["phone"].apply(
            standardize_phone
        )

        logger.info("Customer phone standardization completed")

        cleaned_df = fill_missing_emails(cleaned_df)

        logger.info("Missing customer emails replaced")

        return cleaned_df

    except Exception as error:
        logger.error(
            f"Failed to transform customer data: {error}"
        )
        raise


@flow(name="shopdata-etl-pipeline")
def etl_pipeline():
    customers_df = extract_customers()
    # orders_df = extract_orders()
    # exchange_rates_df = extract_exchange_rates()

    clean_customers_df = transform_customers(customers_df)

    print("\nClean Customers:")
    print(clean_customers_df)

    print(f"\nRaw customer rows: {len(customers_df)}")
    print(f"Clean customer rows: {len(clean_customers_df)}")

    print(
        "Duplicate customer IDs: "
        f"{clean_customers_df['customer_id'].duplicated().sum()}"
    )

    print(
        "Missing emails: "
        f"{clean_customers_df['email'].isna().sum()}"
    )


if __name__ == "__main__":
    etl_pipeline()
