# src/pipelines/silver_payments.py
import pandas as pd

from src.io_raw import load_payments_raw


def _parse_payments_types(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df["payment_date"] = pd.to_datetime(df["payment_date"], errors="coerce")

    df["amount"] = (
        df["amount"]
        .astype(str).str.strip()
        .str.replace(r"\.(?=\d{3}\b)", "", regex=True)  # remove thousand dots
        .str.replace(",", ".", regex=False)            # comma → dot
        .pipe(pd.to_numeric, errors="coerce")
    )

    df["payment_method"] = df["payment_method"].astype(str).str.strip()
    return df


def _split_payments_exceptions(df: pd.DataFrame):
    """
    Basic DQ:
    - payment_date not null
    - order_id not null
    - amount not null
    """
    df = df.copy()

    cond_valid = (
        df["payment_date"].notna()
        & df["order_id"].notna()
        & df["amount"].notna()
    )

    df_valid = df[cond_valid].copy()
    df_exceptions = df[~cond_valid].copy()
    return df_valid, df_exceptions


def build_silver_payments():
    df_raw = load_payments_raw()
    df_typed = _parse_payments_types(df_raw)

    silver_payments, payments_exceptions = _split_payments_exceptions(df_typed)
    return silver_payments, payments_exceptions
