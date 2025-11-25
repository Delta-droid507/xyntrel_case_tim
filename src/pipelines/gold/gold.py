# src/pipelines/gold.py
import pandas as pd

from src.pipelines.silver.silver_orders import build_silver_orders
from src.pipelines.silver.silver_customers import build_silver_customers
from src.pipelines.silver.silver_payments import build_silver_payments
from src.pipelines.silver.silver_rates import build_silver_rates


def _with_eur_amount(df, amount_col, currency_col, date_col, rates_daily):
    df = df.copy()
    df = df.merge(
        rates_daily,
        left_on=date_col,
        right_on="fx_date",
        how="left",
    )

    df["fx_rate"] = 1.0
    df.loc[df[currency_col] == "USD", "fx_rate"] = df["EUR_USD"]
    df.loc[df[currency_col] == "GBP", "fx_rate"] = df["EUR_GBP"]
    df.loc[df[currency_col] == "EUR", "fx_rate"] = 1.0

    df["amount_eur"] = df[amount_col] / df["fx_rate"]
    return df


def _merge_asof_by_group(left, right, by, left_on, right_on, **kwargs):
    """Perform merge_asof within each group defined by `by`.

    Pandas' `merge_asof` requires global sorting of `left_on`, which is
    incompatible when records for multiple groups interleave. This helper
    runs `merge_asof` per-group and concatenates results.
    """
    parts = []
    for key, left_grp in left.groupby(by):
        right_grp = right[right[by] == key]
        if right_grp.empty:
            # join will produce NaNs for right-side columns
            parts.append(left_grp)
            continue
        left_g = left_grp.sort_values(left_on)
        right_g = right_grp.sort_values(right_on)
        merged = pd.merge_asof(left_g, right_g, left_on=left_on, right_on=right_on, **kwargs)
        parts.append(merged)
    if parts:
        return pd.concat(parts, ignore_index=True)
    return left.copy()

def build_gold_monthly_revenue_eur():
    orders, _ = build_silver_orders()
    customers, _ = build_silver_customers()
    fx_daily, _ = build_silver_rates()

    # clean NaT rows first
    orders_clean = orders.dropna(subset=["order_date"]).copy()
    customers_clean = customers.dropna(subset=["effective_from"]).copy()

    # effective-dated join per customer_id
    scd = _merge_asof_by_group(
    left=orders_clean,
    right=customers_clean,
    by="customer_id",
    left_on="order_date",
    right_on="effective_from",
    direction="backward",
    suffixes=("", "_cust"),   # <-- FIX
)

    # 3) keep only rows where the version is still valid at order_date
    mask_dim_valid = scd["effective_to"].isna() | (
        scd["order_date"] < scd["effective_to"]
    )
    scd = scd[mask_dim_valid].copy()

    # 4) convert to EUR using order_date
    scd_fx = _with_eur_amount(
        scd,
        amount_col="total_amount",
        currency_col="currency",
        date_col="order_date",
        rates_daily=fx_daily,
    )

    # 5) keep only completed orders
    rev = scd_fx[scd_fx["status_final"] == "Completed"].copy()

    # 6) aggregate per month per customer
    rev["year_month"] = rev["order_date"].dt.to_period("M").dt.to_timestamp()

    gold = (
        rev.groupby(["customer_id", "year_month"], as_index=False)["amount_eur"]
        .sum()
        .rename(columns={"amount_eur": "revenue_eur"})
    )

    return gold



def build_gold_payment_reconciliation():
    orders, _ = build_silver_orders()
    payments, _ = build_silver_payments()
    fx_daily, _ = build_silver_rates()

    # orders in EUR
    orders_fx = _with_eur_amount(
        orders,
        amount_col="total_amount",
        currency_col="currency",
        date_col="order_date",
        rates_daily=fx_daily,
    )[["order_id", "customer_id", "order_date", "status_final", "amount_eur"]]
    orders_fx = orders_fx.rename(columns={"amount_eur": "order_amount_eur"})

    # attach currency to payments via orders
    payments_join = payments.merge(
        orders[["order_id", "currency"]],
        on="order_id",
        how="left",
    )

    payments_fx = _with_eur_amount(
        payments_join,
        amount_col="amount",
        currency_col="currency",
        date_col="payment_date",
        rates_daily=fx_daily,
    )

    paid_per_order = (
        payments_fx.groupby("order_id", as_index=False)["amount_eur"]
        .sum()
        .rename(columns={"amount_eur": "paid_eur"})
    )

    recon = orders_fx.merge(paid_per_order, on="order_id", how="left")
    recon["paid_eur"] = recon["paid_eur"].fillna(0.0)
    recon["open_amount_eur"] = recon["order_amount_eur"] - recon["paid_eur"]

    return recon
