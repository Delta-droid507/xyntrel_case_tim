# src/runners/run_gold.py
from pathlib import Path

from src.pipelines.gold.gold import (
    build_gold_monthly_revenue_eur,
    build_gold_payment_reconciliation,
)


def main() -> None:
    base_dir = Path(__file__).resolve().parents[2]
    gold_dir = base_dir / "data" / "gold"
    gold_dir.mkdir(parents=True, exist_ok=True)

    monthly = build_gold_monthly_revenue_eur()
    monthly.to_csv(gold_dir / "gold_monthly_revenue_eur.csv", index=False)
    print("Wrote:", gold_dir / "gold_monthly_revenue_eur.csv")

    recon = build_gold_payment_reconciliation()
    recon.to_csv(gold_dir / "gold_payment_reconciliation.csv", index=False)
    print("Wrote:", gold_dir / "gold_payment_reconciliation.csv")


if __name__ == "__main__":
    main()
