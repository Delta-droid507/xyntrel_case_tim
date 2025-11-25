# 📦 Case Instructions (Simplified & Clarified)

You're working with a layered data architecture:
**`raw → silver → gold`**

---

## ✅ Mandatory Steps

1. **Normalize Statuses**
   - Orders with status `pending` for more than 48 hours should be marked as `cancelled`.

2. **Currency Conversion**
   - Convert all amounts to **EUR** using weekly rates from `reference/internal_rates.csv`.
   - Use **forward-fill** for missing rates, but only up to **7 days**.

3. **Customer Dimension (SCD2)**
   - Implement Slowly Changing Dimension Type 2 for customers.
   - Make sure to join the correct customer version based on the `order_date`.

4. **Email Handling**
   - In the **silver layer**, mask emails using **SHA-256** with a **SALT** from `config/.env`.
   - In the **gold layer**, remove all email fields entirely.

5. **Payment Reconciliation**
   - Match payments to orders.
   - Any mismatches or unparseable records should go to either `rejections` or `exceptions`.

---

## 📤 Deliverables

- `gold.monthly_revenue_eur`: Monthly revenue per customer.
- `gold.payment_reconciliation`: Includes `order_amount_eur`, `paid_eur`, and `open_amount_eur`.
- **Data Quality Report**: Include formulated tests and findings.
- **Short Memo to Management Team**: Summarize implications for governance and cost control.

---

🔐 **Note:**
Your personal SALT for hashing is: timo
