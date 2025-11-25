# src/pipelines/silver_rates.py
import pandas as pd

from src.io_raw import load_rates_raw


def build_silver_rates():
    # weekly rates
    df = load_rates_raw().copy()
    df["week_start"] = pd.to_datetime(df["week_start"], errors="coerce")
    df = df.sort_values("week_start")

    # full daily calendar
    start = df["week_start"].min()
    end = df["week_start"].max() + pd.Timedelta(days=6)
    calendar = pd.DataFrame({"date": pd.date_range(start, end, freq="D")})

    daily = calendar.merge(df, left_on="date", right_on="week_start", how="left")

    # forward fill rates
    daily[["EUR_USD", "EUR_GBP"]] = daily[["EUR_USD", "EUR_GBP"]].ffill()

    # track distance from origin week_start
    daily["origin_week_start"] = daily["week_start"].ffill()
    daily["days_since_week"] = (daily["date"] - daily["origin_week_start"]).dt.days

    valid_mask = daily["days_since_week"].between(0, 7)
    rates_valid = daily[valid_mask].copy()
    rates_exceptions = daily[~valid_mask].copy()

    rates_valid = rates_valid.rename(columns={"date": "fx_date"})[
        ["fx_date", "EUR_USD", "EUR_GBP"]
    ]

    return rates_valid, rates_exceptions
