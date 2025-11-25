# Data Dictionary (deels)

**Let op:** sommige kolommen zijn bewust ambigu; verifieer aannames en voeg DQ-tests toe.

## customers.csv (SCD2)
- customer_id (int)
- name (string)
- email (string | null) — in raw mag PII voorkomen; in silver hashed met SHA-256 + SALT; in gold afwezig
- country (string) ∈ {NL, DE, BE, UK, FR}
- effective_from (date)
- effective_to (date|null)
- is_current (int) ∈ {0,1}

## orders.csv
- order_id (int)
- customer_id (int)
- order_date (datetime)
- status (string) ∈ {"Completed","completed","Pending","pending","pndng"}
- total_amount (float|string) — verschillende decimal notaties mogelijk
- currency (string) ∈ {"EUR","USD","GBP"} (GBP verschijnt later in de tijd)

## payments.csv
- payment_id (int)
- order_id (int) — 1% verwijst mogelijk niet naar bestaande order
- payment_date (date)
- amount (float|string) — decimal notatie kan variëren
- payment_method (string|null)

## reference/internal_rates.csv
- week_start (date, maandag)
- EUR_USD (float) — interne wekelijkse koers
- EUR_GBP (float) — idem

---

### Beleid (uittreksel voor kandidaten)
- Pending > 48h telt als cancellation (niet in omzet)
- Valutaconversie via interne wekelijkse koers; forward-fill max 7 dagen; ontbreekt langer → exception
- PII: e-mail in raw toegestaan; in silver gehasht; in gold niet aanwezig
