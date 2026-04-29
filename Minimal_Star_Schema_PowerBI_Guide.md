# Minimal PowerBI Star Schema Guide — Aligned to Current Neo4j Keys

This guide matches the Neo4j property keys visible in the current instance screenshots and keeps the trainer-style schema: one transaction fact table plus three dimensions.

## Exported folders/tables

1. `fact_transactions`
2. `dim_account`
3. `dim_transaction_type`
4. `dim_date`

No extra bridge, KPI, signal, or summary tables are exported.

---

## 1. fact_transactions

### Grain
One row per `TRANSACTION` relationship.

### Source
Neo4j pattern:

```cypher
MATCH (orig:Account)-[t:TRANSACTION]->(dest:Account)
```

### Columns

| Column | Source key | Notes |
|---|---|---|
| `nameOrig` | `orig.id` | Sender account id. |
| `nameDest` | `dest.id` | Recipient account id. |
| `step` | `t.step` | PaySim hour step. Joins to `dim_date.date_key`. |
| `type` | `t.type` | Joins to `dim_transaction_type.type_name`. |
| `amount` | `t.amount` | Transaction amount. |
| `oldbalanceOrg` | `t.oldbalanceOrg` | Sender balance before transaction. |
| `newbalanceOrig` | `t.newbalanceOrig` | Sender balance after transaction. |
| `oldbalanceDest` | `t.oldbalanceDest` | Recipient balance before transaction. |
| `newbalanceDest` | `t.newbalanceDest` | Recipient balance after transaction. |
| `date` | derived from `step` through `dim_date` | Used for partitioning and date visuals. |

### Why no extra transaction flag columns?
The visible transaction relationship keys are only:

`amount`, `newbalanceDest`, `newbalanceOrig`, `oldbalanceDest`, `oldbalanceOrg`, `step`, `type`.

Neo4j also shows internal `<id>` and `<type>`, but those are graph metadata, not project properties needed in PowerBI.

### Best PowerBI uses

- Total transaction volume over time.
- Transaction count by type.
- Amount by transaction type.
- Flagged-volume analysis by joining through sender/recipient account dimensions.

---

## 2. dim_account

### Grain
One row per `Account` node.

### Source
Neo4j pattern:

```cypher
MATCH (a:Account)
```

### Columns

| Column | Source key | Notes |
|---|---|---|
| `account_id` | `a.id` | Renamed from `id` to avoid confusion with Neo4j internal ids. Primary account key. |
| `community_id` | `a.community_id` | Ring/community assignment. Useful for ring visuals. |
| `indegree` | `a.indegree` | Number of incoming connections if written by GDS/query. Present in current Neo4j screenshot. |
| `outdegree` | `a.outdegree` | Number of outgoing connections. Present in current Neo4j screenshot. |
| `fan_out_flag` | `a.fan_out_flag` | Account-level fraud signal. |
| `fan_in_flag` | `a.fan_in_flag` | Account-level fraud signal. |
| `drain_flag` | `a.drain_flag` | Account-level fraud signal. |
| `transfer_cashout_flag` | `a.transfer_cashout_flag` | Account-level fraud signal. |
| `dense_community_flag` | `a.dense_community_flag` | Account-level/ring-membership signal. |
| `cycles_flag` | `a.cycles_flag` | Account-level/ring signal. |
| `association_flag` | `a.association_flag` | Second-pass guilt-by-association flag. |
| `ringtoring_flag` | `a.ringtoring_flag` | Ring-to-ring signal flag. |
| `risk_score` | `a.risk_score` | Final weighted score written back to account. |

### Best PowerBI uses

- Risk score distribution histogram.
- Top 10 riskiest accounts.
- Flag count by signal.
- Community/ring summary by `community_id`.
- Account drill-through page.

---

## 3. dim_transaction_type

### Grain
One row per transaction type.

### Columns

| Column | Notes |
|---|---|
| `type_key` | Small numeric surrogate key generated during export. |
| `type_name` | Transaction type from `t.type`, such as `TRANSFER`, `CASH_OUT`, `PAYMENT`, etc. |

### Best PowerBI uses

- Fraud rate by transaction type.
- Transaction count by type.
- Total amount by type.

---

## 4. dim_date

### Grain
One row per PaySim step.

### Columns

| Column | Notes |
|---|---|
| `date_key` | Equals PaySim `step`. |
| `datetime` | Artificial datetime string generated from `START_DATE + step`. |
| `date` | Artificial date derived from step. |
| `hour` | Hour of day. |
| `day_of_week` | Derived weekday. Artificial, but useful for dashboard slicing. |

### Best PowerBI uses

- Time trend visuals.
- Date slicers.
- Hour-of-day breakdowns.

---

## Recommended PowerBI relationships

Use these relationships:

1. `fact_transactions[step]` many-to-one `dim_date[date_key]`
2. `fact_transactions[type]` many-to-one `dim_transaction_type[type_name]`
3. `fact_transactions[nameOrig]` many-to-one `dim_account[account_id]`
4. `fact_transactions[nameDest]` many-to-one `dim_account[account_id]`

PowerBI may only allow one active relationship from `fact_transactions` to `dim_account`. Keep `nameOrig -> account_id` active first, and use `USERELATIONSHIP()` measures when analyzing recipient accounts.

---

## Practical dashboard mapping

| Required visual | Main table(s) |
|---|---|
| KPI cards | `fact_transactions`, `dim_account` |
| Risk score distribution | `dim_account` |
| Total flagged volume over time | `fact_transactions` + `dim_account` + `dim_date` |
| Fraud/risk by transaction type | `fact_transactions` + `dim_transaction_type` + `dim_account` |
| Top 10 riskiest accounts | `dim_account` |
| Rings moved the most money | `fact_transactions` + `dim_account[community_id]` |

---

## Column audit against current Neo4j screenshots (INTERNAL USE ONLY, REMOVE 04/30 THURSDAY) 

### Account screenshot keys

The aligned driver pulls:

`id`, `community_id`, `indegree`, `outdegree`, `fan_out_flag`, `fan_in_flag`, `drain_flag`, `transfer_cashout_flag`, `dense_community_flag`, `cycles_flag`, `association_flag`, `ringtoring_flag`, `risk_score`.

The export renames only one column:

`a.id` -> `account_id`

### Transaction screenshot keys

The aligned driver pulls:

`amount`, `newbalanceDest`, `newbalanceOrig`, `oldbalanceDest`, `oldbalanceOrg`, `step`, `type`.

It also derives:

`nameOrig` from the sender account id, `nameDest` from the recipient account id, and `date` from `step`.
