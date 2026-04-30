# PowerBI Dashboard Build Steps

## Minimal PaySim Fraud Star Schema

Purpose: build a clean PowerBI dashboard after the Parquet files are exported and the relationships are linked.

This guide assumes the final export contains only the trainer-style tables:

- `fact_transactions`
- `dim_account`
- `dim_transaction_type`
- `dim_date`

---

## Fast Build Order

1. Load the four Parquet folders into PowerBI: `fact_transactions`, `dim_account`, `dim_transaction_type`, and `dim_date`.
2. Create or confirm the relationships in Model View.
3. Create the DAX measures listed below.
4. Build the KPI row first, then the five required analytical visuals.
5. Add slicers and a detailed account table last.
6. During the presentation, explain the risk threshold from the risk score distribution.

---

## 1. Confirm the Relationships

Use `fact_transactions` as the central fact table. The other three tables should filter it.

| From | To | Cardinality | Notes |
|---|---|---|---|
| `fact_transactions[step]` | `dim_date[date_key]` | Many-to-one | Required for date, day, and hour visuals. |
| `fact_transactions[type]` | `dim_transaction_type[type_name]` | Many-to-one | Useful for transaction-type bars and slicers. |
| `fact_transactions[nameOrig]` | `dim_account[account_id]` | Many-to-one | Make this the active account relationship. It describes sender/origin accounts. |
| `fact_transactions[nameDest]` | `dim_account[account_id]` | Many-to-one, optional/inactive | Optional. PowerBI may make this inactive. Use only if you explicitly need recipient-side analysis. |

Important label rule: if the active relationship is `nameOrig -> account_id`, visuals using `dim_account` describe origin/sender accounts. Label charts as **Origin** or **Sender** to avoid overstating the result.

---

## 2. Create the Core Measures

In PowerBI, go to **Modeling -> New measure**. Create these measures first.

### Total Transactions

```DAX
Total Transactions =
COUNTROWS(fact_transactions)
```

### Total Amount

```DAX
Total Amount =
SUM(fact_transactions[amount])
```

### Total Accounts

```DAX
Total Accounts =
DISTINCTCOUNT(dim_account[account_id])
```

### Flagged Accounts

```DAX
Flagged Accounts =
CALCULATE(
    DISTINCTCOUNT(dim_account[account_id]),
    dim_account[risk_score] > 0
)
```

### High Risk Accounts

```DAX
High Risk Accounts =
CALCULATE(
    DISTINCTCOUNT(dim_account[account_id]),
    dim_account[risk_score] > 10
)
```

### Rings Detected

```DAX
Rings Detected =
CALCULATE(
    DISTINCTCOUNT(dim_account[community_id]),
    dim_account[dense_community_flag] = 1
)
```

### Flagged Origin Transaction Count

```DAX
Flagged Origin Transaction Count =
CALCULATE(
    [Total Transactions],
    dim_account[risk_score] > 0
)
```

### Flagged Origin Volume

```DAX
Flagged Origin Volume =
CALCULATE(
    [Total Amount],
    dim_account[risk_score] > 0
)
```

### Flagged Origin Rate

```DAX
Flagged Origin Rate =
DIVIDE(
    [Flagged Origin Transaction Count],
    [Total Transactions]
)
```

---

## 3. Build the Dashboard Visuals

Build the visuals in this order. This gives the dashboard a clear story:

**Scale -> risk distribution -> specific accounts -> time/type/ring behavior**

| Step / Visual | PowerBI visual | Fields | Filters / setup | Why it matters |
|---|---|---|---|---|
| A. KPI Cards | Card visuals | `Total Transactions`; `Total Amount`; `Total Accounts`; `Flagged Accounts`; `High Risk Accounts`; `Rings Detected`; `Flagged Origin Volume` | Top row of the page | Gives audience the scale of the data before looking at detailed patterns. |
| B. Risk Score Distribution | Column chart or histogram | X-axis: `dim_account[risk_score]` or binned `risk_score`. Y-axis: count of `account_id`. | Create bins of 5 or 10 if the x-axis is messy. | Used to justify the high-risk threshold. |
| C. Top 10 Riskiest Accounts | Horizontal bar chart | Y-axis: `dim_account[account_id]`. X-axis: max `risk_score`. | Visual filter: Top N = 10 by max `risk_score`. | Best chart for case-study selection. |
| D. Flagged Origin Volume Over Time | Line chart | X-axis: `dim_date[date]` or `dim_date[date_key]`. Y-axis: `Flagged Origin Volume`. Optional legend: `type_name`. | If too crowded, remove the legend. | Shows when suspicious sender-side volume occurs. |
| E. Flagged Origin Rate by Type | Bar chart | X-axis: `dim_transaction_type[type_name]`. Y-axis: `Flagged Origin Rate`. | Format Y-axis as percentage. Add `Total Transactions` to tooltip. | Shows which transaction types are most associated with flagged origin accounts. |
| F. Dense Communities / Rings | Bar chart | Y-axis: `dim_account[community_id]`. X-axis: `Total Amount`. | Filter: `dense_community_flag = 1`. Top N = 10 by `Total Amount`. | Shows which flagged communities/rings moved the most sender-side volume. |
| G. Detailed Account Table | Table visual | `account_id`, `risk_score`, `community_id`, `indegree`, `outdegree`, all flag columns. | Sort `risk_score` descending. Add conditional formatting to `risk_score`. | Lets you explain exactly which flags fired for an account. |

---

## 4. Recommended One-Page Layout

Use one main dashboard page. Keep it simple and presentation-friendly.

| Area | Visuals |
|---|---|
| Top row | KPI cards: `Total Transactions`, `Total Amount`, `Total Accounts`, `Flagged Accounts`, `High Risk Accounts`, `Rings Detected` |
| Middle left | Risk Score Distribution |
| Middle right | Top 10 Riskiest Accounts |
| Bottom left | Flagged Origin Volume Over Time |
| Bottom middle | Flagged Origin Rate by Transaction Type |
| Bottom right | Top Dense Communities by Sender Transaction Volume |
| Side or second page | Detailed Account Table |

---

## 5. Add Slicers

Add these after the main visuals are working:

- `dim_date[date]` for time filtering.
- `dim_transaction_type[type_name]` for transaction-type filtering.
- `dim_account[risk_score]` as a slider or dropdown to test risk thresholds.
- `dim_account[community_id]` for ring/community drilldowns.

---

## 6. Common Mistakes to Avoid

- Do not import `_fact_transactions_raw`. It is only an intermediate folder if it exists during export.
- Do not create extra summary tables unless the trainer specifically allows it. The four-table model is enough for this dashboard.
- Do not call sender-side flagged volume “total fraud volume” unless you also implement recipient-side logic. Label it **Flagged Origin Volume**.
- Do not use Neo4j internal IDs in PowerBI. Use `account_id` from `dim_account` and `nameOrig` / `nameDest` from `fact_transactions`.
- Do not overcomplicate the risk threshold. Use the histogram and pick a defensible break, such as `risk_score > 10` if the distribution supports it.

---

## 7. Suggested Presentation Walkthrough

1. Start with KPI cards: total transactions, accounts, flagged accounts, and total amount.
2. Move to the risk score histogram and explain how the threshold was chosen.
3. Show the top 10 riskiest accounts and point out which flags caused the high score.
4. Show flagged origin volume over time and identify any spikes.
5. Show transaction type risk/rate and discuss whether `TRANSFER` or `CASH_OUT` dominates.
6. Show dense communities/rings by sender-side volume.
7. End with one account or community case study using the detailed table and, if helpful, a Neo4j Browser screenshot.

---

## Appendix: Field Checklist

| Table | Important fields |
|---|---|
| `fact_transactions` | `nameOrig`, `nameDest`, `step`, `type`, `amount`, `oldbalanceOrg`, `newbalanceOrig`, `oldbalanceDest`, `newbalanceDest`, `date` |
| `dim_account` | `account_id`, `community_id`, `indegree`, `outdegree`, `risk_score`, `fan_out_flag`, `fan_in_flag`, `drain_flag`, `transfer_cashout_flag`, `dense_community_flag`, `cycles_flag`, `association_flag`, `ringtoring_flag` |
| `dim_transaction_type` | `type_key`, `type_name` |
| `dim_date` | `date_key`, `datetime`, `date`, `hour`, `day_of_week` |
