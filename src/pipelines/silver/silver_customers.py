# src/pipelines/silver_customers.py
import hashlib
import pandas as pd

from src.io_raw import load_customers_raw
from src.config import SALT

VALID_COUNTRIES = {"NL", "DE", "BE", "UK", "FR"}


def _parse_customers_types(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["effective_from"] = pd.to_datetime(df["effective_from"], errors="coerce")
    df["effective_to"] = pd.to_datetime(df["effective_to"], errors="coerce")
    df["country"] = df["country"].astype(str).str.strip()

    # keep as nullable integer (0/1/NaN)
    df["is_current"] = (
        pd.to_numeric(df["is_current"], errors="coerce")
        .astype("Int64")
    )
    return df


def _hash_email(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    def _hash(val) -> str | None:
        if pd.isna(val):
            return None
        email_norm = str(val).strip().lower()
        data = (SALT + email_norm).encode("utf-8")
        return hashlib.sha256(data).hexdigest()

    df["email_hash"] = df["email"].map(_hash)
    return df


def _split_customers_exceptions(df: pd.DataFrame):
    """
    Basic DQ:
    - customer_id not null
    - effective_from not null
    - country in allowed set
    - is_current in {0, 1}
    """
    df = df.copy()

    cond_valid = (
        df["customer_id"].notna()
        & df["effective_from"].notna()
        & df["country"].isin(VALID_COUNTRIES)
        & df["is_current"].isin([0, 1])
    )

    df_valid = df[cond_valid].copy()
    df_exceptions = df[~cond_valid].copy()
    return df_valid, df_exceptions


def build_silver_customers():
    df_raw = load_customers_raw()
    df_typed = _parse_customers_types(df_raw)
    df_hashed = _hash_email(df_typed)

    silver_customers, customers_exceptions = _split_customers_exceptions(df_hashed)
    return silver_customers, customers_exceptions
