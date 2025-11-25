# src/io_raw.py
import pandas as pd
from src.config import RAW_DIR, REF_DIR


def load_customers_raw() -> pd.DataFrame:
    return pd.read_csv(RAW_DIR / "customers.csv")

def load_orders_raw() -> pd.DataFrame:
    return pd.read_csv(RAW_DIR / "orders.csv")

def load_payments_raw() -> pd.DataFrame:
    return pd.read_csv(RAW_DIR / "payments.csv")

def load_rates_raw() -> pd.DataFrame:
    return pd.read_csv(REF_DIR / "internal_rates.csv")
