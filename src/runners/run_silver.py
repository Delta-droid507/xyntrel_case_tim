# src/runners/run_silver.py
from pathlib import Path

from src.pipelines.silver_orders import build_silver_orders
from src.pipelines.silver_customers import build_silver_customers
from src.pipelines.silver_payments import build_silver_payments
# from src.pipelines.silver_rates import build_silver_rates  # later


def main() -> None:
    base_dir = Path(__file__).resolve().parents[2]
    silver_dir = base_dir / "data" / "silver"
    silver_dir.mkdir(parents=True, exist_ok=True)

    # ---- Orders ----
    silver_orders, orders_exceptions = build_silver_orders()
    silver_orders.to_csv(silver_dir / "silver_orders.csv", index=False)
    orders_exceptions.to_csv(
        silver_dir / "silver_orders_exceptions.csv", index=False
    )
    print("Wrote:", silver_dir / "silver_orders.csv")
    print("Wrote:", silver_dir / "silver_orders_exceptions.csv")

    # ---- Customers ----
    silver_customers, customers_exceptions = build_silver_customers()
    silver_customers.to_csv(silver_dir / "silver_customers.csv", index=False)
    customers_exceptions.to_csv(
        silver_dir / "silver_customers_exceptions.csv", index=False
    )
    print("Wrote:", silver_dir / "silver_customers.csv")
    print("Wrote:", silver_dir / "silver_customers_exceptions.csv")

    # ---- Payments ----
    silver_payments, payments_exceptions = build_silver_payments()
    silver_payments.to_csv(silver_dir / "silver_payments.csv", index=False)
    payments_exceptions.to_csv(
        silver_dir / "silver_payments_exceptions.csv", index=False
    )
    print("Wrote:", silver_dir / "silver_payments.csv")
    print("Wrote:", silver_dir / "silver_payments_exceptions.csv")

    # ---- Rates (later) ----
    # rates, rates_exceptions = build_silver_rates()
    # rates.to_csv(silver_dir / "silver_rates.csv", index=False)
    # rates_exceptions.to_csv(
    #     silver_dir / "silver_rates_exceptions.csv", index=False
    # )


if __name__ == "__main__":
    main()
