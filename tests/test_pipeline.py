import pandas as pd
import pytest

from pipeline import (
    filter_invalid_orders,
    standardize_phone,
    convert_orders_to_usd,
)


def test_standardize_phone_removes_non_numeric_characters():
    assert standardize_phone("+1 (555) 123-4567") == "15551234567"
    assert standardize_phone("555-111-2222") == "5551112222"
    assert standardize_phone("1-800-555-DINO") == "1800555"


def test_standardize_phone_handles_missing_value():
    assert standardize_phone(None) is None


def test_filter_invalid_orders():
    orders_df = pd.DataFrame(
        {
            "order_id": [1, 2, 3],
            "total_amount": [100.0, 0.0, -50.0],
        }
    )

    result_df = filter_invalid_orders(orders_df)

    assert len(result_df) == 1
    assert result_df.iloc[0]["order_id"] == 1
    assert result_df.iloc[0]["total_amount"] == 100.0


def test_convert_orders_to_usd():
    orders_df = pd.DataFrame(
        {
            "order_id": [1, 2, 3],
            "customer_id": [1, 2, 3],
            "order_date": [
                "2023-05-01",
                "2023-05-01",
                "2023-05-01",
            ],
            "total_amount": [
                100.0,
                200.0,
                300.0,
            ],
            "currency": [
                "EUR",
                None,
                "GBP",
            ],
            "status": [
                "COMPLETED",
                "COMPLETED",
                "COMPLETED",
            ],
        }
    )

    exchange_rates_df = pd.DataFrame(
        {
            "currency": ["EUR"],
            "rate_to_usd": [1.10],
            "date": ["2023-05-01"],
        }
    )

    result_df = convert_orders_to_usd(
        orders_df,
        exchange_rates_df,
    )

    # EUR has a matching exchange rate
    assert result_df.iloc[0]["usd_amount"] == pytest.approx(110.0)

    # Missing currency -> assume already USD
    assert result_df.iloc[1]["usd_amount"] == pytest.approx(200.0)

    # GBP has no matching exchange rate -> assume already USD
    assert result_df.iloc[2]["usd_amount"] == pytest.approx(300.0)
