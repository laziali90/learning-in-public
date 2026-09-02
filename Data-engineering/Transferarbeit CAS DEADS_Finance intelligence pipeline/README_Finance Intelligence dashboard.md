# TEAM UMCC Finance Intelligence Dashboard

## Overview

A multi-view dashboard for financial analysis of UEFA Club Competitions, providing insights into partner rights fees, payment diligence, and competition budget management. Built on two independent data pipelines feeding distinct analytical views.

---

## View 1: Payment & Rights Fees

### Data Source
**Pipeline:** `team.gold.payment`  
**Dataset Type:** Local Metric View

### Ingested Fields (Dimensions)

| Source Field | Dashboard Field | Transformation |
|-------------|----------------|----------------|
| `Partner` | Partner | Direct mapping |
| `Partner_Category` | Partner Category | Direct mapping |
| `Season` | Season | Direct mapping |
| `competition` | Competition | Direct mapping |
| `invoice_number` | Invoice Number | Direct mapping |
| `Invoiced_date` | Invoiced Date | Direct mapping |
| `Payment_received_date` | Payment Received Date | Direct mapping |
| — | Days to Payment | Calculated: `DATEDIFF(day, Invoiced_date, Payment_received_date)` |
| `Payment_Diligence_Invoice` | Payment Diligence Invoice | Direct mapping |
| `Payment_Diligence_Season` | Payment Diligence Season | Direct mapping |
| `Payment_received_in_days` | Payment Timing Days | Direct mapping |
| `invoiced_eur` | Invoiced (EUR) | Scaled: `source * 1000` |
| `cycle` | Cycle | Direct mapping |

### Measures

| Measure Name | Expression | Display Name |
|-------------|-----------|--------------|
| `total_rights_fee` | `SUM(source.Rights_fee) * 1000` | Total Rights Fee (EUR) |
| `total_invoiced` | `SUM(source.invoiced_eur) * 1000` | Total Invoiced (EUR) |
| `invoice_count` | `COUNT_IF(source.invoice_number IS NOT NULL)` | Invoices count |
| `paid_invoice_count` | `COUNT_IF(source.Payment_received_date IS NOT NULL)` | Paid Invoices Count |
| `unpaid_invoice_count` | `COUNT_IF(source.Payment_received_date IS NULL)` | Unpaid Invoice Count |
| `prompt_payer_count` | `COUNT_IF(source.Payment_Diligence_Invoice = 'Prompt Payer')` | PromptPayer Count |
| `slow_payer_count` | `COUNT_IF(source.Payment_Diligence_Invoice = 'Slow Payer')` | SlowPayer Count |

### Transformation Logic

**Naming Convention:** Source fields use snake_case and abbreviations; dashboard dimensions are renamed to Title Case with full words for readability.

**Value Scaling:** Financial values (`Rights_fee`, `invoiced_eur`) are multiplied by 1000 to convert from thousands to actual EUR amounts.

**Calculated Dimensions:** The "Days to Payment" field is computed per invoice as the difference between Invoiced Date and Payment Received Date, enabling row-level payment timing analysis.

**Conditional Aggregations:** Payment diligence measures use `COUNT_IF()` to segment invoice counts by payment behavior (prompt vs. slow payers) and payment status (paid vs. unpaid).

---

## View 2: DCB

### Data Source
**Pipeline:** `team.gold.dcb`  
**Dataset Type:** Local Metric View

### Ingested Fields (Dimensions)

| Source Field | Dashboard Field | Transformation |
|-------------|----------------|----------------|
| `season` | Season | Direct mapping |
| `Budget_category` | Budget Category | Direct mapping |
| `Budget_line` | Budget Line | Direct mapping |
| `competition` | Competition | Direct mapping |
| `document_date` | Document Date | Direct mapping |
| `_source_file` | Source File | Direct mapping |
| `_ingested_at` | Ingested At | Direct mapping |
| `row_id` | Row ID | Direct mapping |
| `cycle` | Cycle | Direct mapping |
| `budget_accuracy_sum` | Budget Accuracy Summary | Direct mapping (categorical) |
| `Cost_main_category` | Cost Main Category | Direct mapping |
| `budget_accuracy_calc` | Budget Accuracy Calc (%) | Direct mapping (numeric) |

### Measures

| Measure Name | Expression | Display Name |
|-------------|-----------|--------------|
| `count` | `COUNT(*)` | Count |
| `total_initial_budget` | `SUM(Initial_total_budget_EUR)` | Total Initial Budget (EUR) |
| `total_forecast_budget` | `SUM(Total_budget_last_forecast_EUR)` | Total Forecast Budget (EUR) |
| `total_expected_spend` | `SUM(Total_expected_spend_EUR)` | Total Spend (EUR) |
| `total_budget_difference` | `SUM(Budget_difference)` | Total Budget Difference (EUR) |

### Transformation Logic

**Naming Convention:** Source fields maintain snake_case structure; dashboard fields use Title Case and expanded terminology (e.g., `Budget_category` → Budget Category).

**Aggregation Strategy:** All budget measures use `SUM()` to aggregate across budget lines, enabling rollup by category and season. Budget accuracy is preserved as both a categorical summary (`budget_accuracy_sum`) and numeric percentage (`budget_accuracy_calc`) for dual analysis perspectives.

**Metadata Preservation:** Pipeline metadata fields (`_source_file`, `_ingested_at`) are surfaced as dimensions for data lineage and audit trail.

---

## Cross-View Design Principles

1. **Independent Data Models:** Each view operates on a dedicated metric view with no cross-dataset relationships, ensuring pipeline autonomy and performance isolation.

2. **Shared Filter Dimensions:** Both views expose `Season`, `Competition`, and `Cycle` for consistent temporal filtering across the dashboard.

3. **Metric View Pattern:** All aggregations are pre-defined in the metric view layer, ensuring consistent calculation logic and query performance through measure reuse.

4. **Row-Level Calculations:** Computed dimensions (e.g., Days to Payment) are evaluated per row, enabling granular analysis without aggregation constraints.
