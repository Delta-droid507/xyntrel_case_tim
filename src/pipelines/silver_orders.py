# src/pipelines/silver_orders.py
import pandas as pd
from src.io_raw import load_orders_raw


STATUS_MAP = {
    "Completed": "Completed",
    "completed": "Completed",
    "Pending":   "Pending",
    "pending":   "Pending",
    "pndng":     "Pending",
}

VALID_CURRENCIES = {"EUR", "USD", "GBP"}

def _parse_orders_types(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")
    df["total_amount"] = (
        df["total_amount"]
        .astype(str).str.strip()
        .str.replace(r"\.(?=\d{3}\b)", "", regex=True)  # drop thousand dots
        .str.replace(",", ".", regex=False)            # comma → dot
        .pipe(pd.to_numeric, errors="coerce")
    )
    df["currency"] = df["currency"].astype(str).str.strip()
    return df


def _normalize_status(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["status_norm"] = df["status"].map(STATUS_MAP)
    return df

def _apply_pending_cancellation(df: pd.DataFrame, now_ts: pd.Timestamp) -> pd.DataFrame:
    """
    Simplest interpretation: if an order is still Pending and older than 48h
    relative to now_ts, mark as Cancelled.
    """
    df = df.copy()
    age_hours = (now_ts - df["order_date"]).dt.total_seconds() / 3600
    df["status_final"] = df["status_norm"]
    mask_cancel = (df["status_norm"] == "Pending") & (age_hours > 48)
    df.loc[mask_cancel, "status_final"] = "Cancelled"
    return df

def _split_orders_exceptions(df: pd.DataFrame):
    """
    Basic DQ based on orders_checks.json:
    - order_date not null
    - customer_id not null
    - currency in allowed set
    """
    df = df.copy()
    cond_valid = (
        df["order_date"].notna()
        & df["customer_id"].notna()
        & df["currency"].isin(VALID_CURRENCIES)
    )
    df_valid = df[cond_valid].copy()
    df_exceptions = df[~cond_valid].copy()
    return df_valid, df_exceptions

def build_silver_orders(now_ts: pd.Timestamp | None = None):
    if now_ts is None:
        # use naive timestamp (no tz) to match order_date
        now_ts = pd.Timestamp.utcnow().tz_localize(None)

    df_raw = load_orders_raw()
    df_typed = _parse_orders_types(df_raw)
    df_status = _normalize_status(df_typed)
    df_status = _apply_pending_cancellation(df_status, now_ts)

    silver_orders, orders_exceptions = _split_orders_exceptions(df_status)

    return silver_orders, orders_exceptions
