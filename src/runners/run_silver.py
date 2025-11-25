from pathlib import Path

# Import all silver pipelines
from src.pipelines.silver_orders import build_silver_orders
# (later: from src.pipelines.silver_rates import build_silver_rates)
# (later: from src.pipelines.silver_customers import build_silver_customers)
# (later: from src.pipelines.silver_payments import build_silver_payments)

def main() -> None:
    # project root: from src/runners/ → up two levels
    base_dir = Path(__file__).resolve().parents[2]
    silver_dir = base_dir / "data" / "silver"
    silver_dir.mkdir(exist_ok=True)

    # ---- Orders ----
    silver_orders, orders_exceptions = build_silver_orders()
    silver_orders.to_csv(silver_dir / "silver_orders.csv", index=False)
    orders_exceptions.to_csv(silver_dir / "silver_orders_exceptions.csv", index=False)
    print("Wrote: silver/silver_orders.csv")
    print("Wrote: silver/silver_orders_exceptions.csv")

    # ---- Later: Rates ----
    # rates, rates_exceptions = build_silver_rates()
    # rates.to_csv(silver_dir / "silver_rates.csv", index=False)
    # rates_exceptions.to_csv(silver_dir / "silver_rates_exceptions.csv", index=False)

    # ---- Later: Customers (SCD2 + hashing) ----
    # customers, customers_exceptions = build_silver_customers()
    # customers.to_csv(silver_dir / "silver_customers.csv", index=False)
    # customers_exceptions.to_csv(silver_dir / "silver_customers_exceptions.csv", index=False)

    # ---- Later: Payments ----
    # payments, payments_exceptions = build_silver_payments()
    # payments.to_csv(silver_dir / "silver_payments.csv", index=False)
    # payments_exceptions.to_csv(silver_dir / "silver_payments_exceptions.csv", index=False)

if __name__ == "__main__":
    main()

