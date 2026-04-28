"""
batching_python_driver.py

Neo4j PaySim graph -> Parquet tables for a PowerBI dashboard.

This version is customized for Team 2's graph property keys:
  Account node properties:
    id, community_id, outdegree,
    fan_out_flag, fan_in_flag, drain_flag, transfer_cashout_flag,
    dense_community_flag, cycles_flag, association_flag, ringtoring_flag,
    risk_score

  Transaction relationship properties:
    step, type, amount,
    oldbalanceOrg, newbalanceOrig, oldbalanceDest, newbalanceDest,
    optionally isFraud, isFlaggedFraud if those were loaded from PaySim

Outputs Parquet folders under ./Output_Parquet:
  dim_account
  dim_transaction_type
  dim_date
  dim_signal
  bridge_account_signal
  dim_community
  fact_transactions                 partitioned by date
  fact_community_flow
  fact_dashboard_kpis
  fact_risk_score_distribution
  fact_signal_transaction_summary

Design notes:
  - fact_transactions grain = 1 Neo4j TRANSACTION relationship.
  - dim_account grain = 1 account node.
  - bridge_account_signal grain = 1 fired signal per account.
  - dim_community supports ring/community visuals.
  - fact_community_flow supports ring-to-ring/internal-vs-external money flow visuals.
  - Account and transaction reads use keyset pagination over elementId(), avoiding SKIP/LIMIT.

Run:
  python batching_python_driver.py

Optional environment variables:
  NEO4J_URI=bolt://localhost:7687
  NEO4J_USER=neo4j
  NEO4J_PASSWORD=password
  OUTPUT_DIR=./Output_Parquet
  START_DATE=2026-03-01
  BATCH_SIZE=25000
  ACCOUNT_BATCH_SIZE=50000
  HIGH_RISK_THRESHOLD=10
"""

import os
import sys
import shutil
from datetime import datetime, timedelta
from pathlib import Path

# Spark needs to know which Python executable to use, especially on Windows.
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from neo4j import GraphDatabase
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    LongType,
    StringType,
    DoubleType,
    DateType,
)

# -----------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "password")

# PaySim has step = hour number. This date is artificial and only exists so
# PowerBI can use normal Date slicers/axes.
START_DATE = datetime.strptime(os.getenv("START_DATE", "2026-03-01"), "%Y-%m-%d")

OUTPUT_DIR = Path(os.getenv("OUTPUT_DIR", "./Output_Parquet"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "25000"))
ACCOUNT_BATCH_SIZE = int(os.getenv("ACCOUNT_BATCH_SIZE", "50000"))
HIGH_RISK_THRESHOLD = float(os.getenv("HIGH_RISK_THRESHOLD", "10"))

# Team 2 risk formula, matching write_risk.md.
SIGNALS = [
    {
        "signal_key": 1,
        "signal_name": "Unusual fan-out",
        "flag_column": "fan_out_flag",
        "transaction_column": "has_fan_out_endpoint",
        "weight": 10,
        "description": "Account sends to unusually many destinations. Team found no suspicious fan-out and set this to 0 by default.",
    },
    {
        "signal_key": 2,
        "signal_name": "Unusual fan-in",
        "flag_column": "fan_in_flag",
        "transaction_column": "has_fan_in_endpoint",
        "weight": 10,
        "description": "Account receives from unusually many sources in a single step.",
    },
    {
        "signal_key": 3,
        "signal_name": "Drain behavior",
        "flag_column": "drain_flag",
        "transaction_column": "has_drain_endpoint",
        "weight": 15,
        "description": "Account receives money and nearly empties shortly after.",
    },
    {
        "signal_key": 4,
        "signal_name": "Transfer followed by cash-out",
        "flag_column": "transfer_cashout_flag",
        "transaction_column": "has_transfer_cashout_endpoint",
        "weight": 15,
        "description": "TRANSFER followed by CASH_OUT within a short step window with similar amount.",
    },
    {
        "signal_key": 5,
        "signal_name": "Dense suspicious community",
        "flag_column": "dense_community_flag",
        "transaction_column": "has_dense_community_endpoint",
        "weight": 20,
        "description": "Member of a small community with very high internal-to-external volume ratio.",
    },
    {
        "signal_key": 6,
        "signal_name": "Cycle membership",
        "flag_column": "cycles_flag",
        "transaction_column": "has_cycles_endpoint",
        "weight": 20,
        "description": "Account participates in a transaction cycle. Team found no cycles and set this to 0 by default.",
    },
    {
        "signal_key": 7,
        "signal_name": "Guilt by association",
        "flag_column": "association_flag",
        "transaction_column": "has_association_endpoint",
        "weight": 10,
        "description": "Previously unflagged account transacts with at least one flagged neighbor.",
    },
    {
        "signal_key": 8,
        "signal_name": "Ring-to-ring flow",
        "flag_column": "ringtoring_flag",
        "transaction_column": "has_ringtoring_endpoint",
        "weight": 10,
        "description": "Account is involved in money flow between suspicious communities. Team query returned empty unless later updated.",
    },
]

SIGNAL_COLUMNS = [s["flag_column"] for s in SIGNALS]
SIGNAL_BY_COLUMN = {s["flag_column"]: s for s in SIGNALS}

# -----------------------------------------------------------------------------
# Schemas
# -----------------------------------------------------------------------------
ACCOUNT_BATCH_SCHEMA = StructType([
    StructField("cursor", StringType(), False),
    StructField("account_id", StringType(), False),
    StructField("account_type", StringType(), False),
    StructField("community_id", StringType(), False),
    StructField("outdegree", IntegerType(), True),
    StructField("fan_out_flag", IntegerType(), False),
    StructField("fan_in_flag", IntegerType(), False),
    StructField("drain_flag", IntegerType(), False),
    StructField("transfer_cashout_flag", IntegerType(), False),
    StructField("dense_community_flag", IntegerType(), False),
    StructField("cycles_flag", IntegerType(), False),
    StructField("association_flag", IntegerType(), False),
    StructField("ringtoring_flag", IntegerType(), False),
    StructField("risk_score", DoubleType(), False),
    StructField("is_flagged_account", IntegerType(), False),
    StructField("is_high_risk_account", IntegerType(), False),
])
ACCOUNT_SCHEMA = StructType([f for f in ACCOUNT_BATCH_SCHEMA.fields if f.name != "cursor"])

BRIDGE_ACCOUNT_SIGNAL_SCHEMA = StructType([
    StructField("account_id", StringType(), False),
    StructField("signal_key", IntegerType(), False),
    StructField("signal_name", StringType(), False),
    StructField("flag_column", StringType(), False),
    StructField("flag_value", IntegerType(), False),
    StructField("signal_weight", IntegerType(), False),
    StructField("signal_points", IntegerType(), False),
])

TRANSACTION_BATCH_SCHEMA = StructType([
    StructField("cursor", StringType(), False),
    StructField("transaction_id", StringType(), False),
    StructField("nameOrig", StringType(), False),
    StructField("nameDest", StringType(), False),
    StructField("step", IntegerType(), False),
    StructField("type", StringType(), False),
    StructField("amount", DoubleType(), False),
    StructField("oldbalanceOrg", DoubleType(), True),
    StructField("newbalanceOrig", DoubleType(), True),
    StructField("oldbalanceDest", DoubleType(), True),
    StructField("newbalanceDest", DoubleType(), True),
    StructField("is_fraud", IntegerType(), True),
    StructField("is_flagged_fraud", IntegerType(), True),
    StructField("orig_community_id", StringType(), False),
    StructField("dest_community_id", StringType(), False),
    StructField("orig_risk_score", DoubleType(), False),
    StructField("dest_risk_score", DoubleType(), False),
    StructField("orig_is_flagged_account", IntegerType(), False),
    StructField("dest_is_flagged_account", IntegerType(), False),
    StructField("has_flagged_endpoint", IntegerType(), False),
    StructField("has_high_risk_endpoint", IntegerType(), False),
    StructField("flagged_endpoint_amount", DoubleType(), False),
    StructField("high_risk_endpoint_amount", DoubleType(), False),
    StructField("cross_community_flag", IntegerType(), False),
    StructField("has_fan_out_endpoint", IntegerType(), False),
    StructField("has_fan_in_endpoint", IntegerType(), False),
    StructField("has_drain_endpoint", IntegerType(), False),
    StructField("has_transfer_cashout_endpoint", IntegerType(), False),
    StructField("has_dense_community_endpoint", IntegerType(), False),
    StructField("has_cycles_endpoint", IntegerType(), False),
    StructField("has_association_endpoint", IntegerType(), False),
    StructField("has_ringtoring_endpoint", IntegerType(), False),
])
TRANSACTION_SCHEMA = StructType([f for f in TRANSACTION_BATCH_SCHEMA.fields if f.name != "cursor"])

DIM_TYPE_SCHEMA = StructType([
    StructField("type_key", IntegerType(), False),
    StructField("type_name", StringType(), False),
])

DIM_DATE_SCHEMA = StructType([
    StructField("date_key", IntegerType(), False),
    StructField("datetime", StringType(), False),
    StructField("date", DateType(), False),
    StructField("hour", IntegerType(), False),
    StructField("day", IntegerType(), False),
    StructField("day_of_week", StringType(), False),
    StructField("week_number", IntegerType(), False),
])

DIM_SIGNAL_SCHEMA = StructType([
    StructField("signal_key", IntegerType(), False),
    StructField("signal_name", StringType(), False),
    StructField("flag_column", StringType(), False),
    StructField("transaction_column", StringType(), False),
    StructField("weight", IntegerType(), False),
    StructField("description", StringType(), False),
])

COMMUNITY_BASE_SCHEMA = StructType([
    StructField("community_id", StringType(), False),
    StructField("member_count", LongType(), False),
    StructField("flagged_member_count", LongType(), False),
    StructField("high_risk_member_count", LongType(), False),
    StructField("dense_community_member_count", LongType(), False),
    StructField("avg_risk_score", DoubleType(), False),
    StructField("max_risk_score", DoubleType(), False),
])

COMMUNITY_FLOW_SCHEMA = StructType([
    StructField("from_community_id", StringType(), False),
    StructField("to_community_id", StringType(), False),
    StructField("is_internal_flow", IntegerType(), False),
    StructField("both_suspicious_endpoint_members", IntegerType(), False),
    StructField("tx_count", LongType(), False),
    StructField("total_amount", DoubleType(), False),
    StructField("flagged_endpoint_amount", DoubleType(), False),
])

# -----------------------------------------------------------------------------
# Cypher
# -----------------------------------------------------------------------------
ACCOUNTS_QUERY = """
MATCH (a:Account)
WHERE elementId(a) > $last_cursor
WITH a
RETURN
  elementId(a) AS cursor,
  toString(a.id) AS account_id,
  CASE
    WHEN toString(a.id) STARTS WITH 'C' THEN 'Customer'
    WHEN toString(a.id) STARTS WITH 'M' THEN 'Merchant'
    ELSE 'Other'
  END AS account_type,
  coalesce(toString(a.community_id), 'UNASSIGNED') AS community_id,
  toInteger(coalesce(a.outdegree, 0)) AS outdegree,
  toInteger(coalesce(a.fan_out_flag, 0)) AS fan_out_flag,
  toInteger(coalesce(a.fan_in_flag, 0)) AS fan_in_flag,
  toInteger(coalesce(a.drain_flag, 0)) AS drain_flag,
  toInteger(coalesce(a.transfer_cashout_flag, 0)) AS transfer_cashout_flag,
  toInteger(coalesce(a.dense_community_flag, 0)) AS dense_community_flag,
  toInteger(coalesce(a.cycles_flag, 0)) AS cycles_flag,
  toInteger(coalesce(a.association_flag, 0)) AS association_flag,
  toInteger(coalesce(a.ringtoring_flag, 0)) AS ringtoring_flag,
  toFloat(coalesce(a.risk_score, 0.0)) AS risk_score,
  CASE WHEN toFloat(coalesce(a.risk_score, 0.0)) > 0 THEN 1 ELSE 0 END AS is_flagged_account,
  CASE WHEN toFloat(coalesce(a.risk_score, 0.0)) >= $high_risk_threshold THEN 1 ELSE 0 END AS is_high_risk_account
ORDER BY elementId(a)
LIMIT $batch_size
"""

TRANSACTIONS_QUERY = """
MATCH (orig:Account)-[t:TRANSACTION]->(dest:Account)
WHERE elementId(t) > $last_cursor
WITH orig, t, dest,
     toFloat(coalesce(orig.risk_score, 0.0)) AS orig_risk,
     toFloat(coalesce(dest.risk_score, 0.0)) AS dest_risk,
     coalesce(toString(orig.community_id), 'UNASSIGNED') AS orig_comm,
     coalesce(toString(dest.community_id), 'UNASSIGNED') AS dest_comm,
     toFloat(coalesce(t.amount, 0.0)) AS amt,
     toInteger(coalesce(orig.fan_out_flag, 0)) AS orig_fan_out,
     toInteger(coalesce(dest.fan_out_flag, 0)) AS dest_fan_out,
     toInteger(coalesce(orig.fan_in_flag, 0)) AS orig_fan_in,
     toInteger(coalesce(dest.fan_in_flag, 0)) AS dest_fan_in,
     toInteger(coalesce(orig.drain_flag, 0)) AS orig_drain,
     toInteger(coalesce(dest.drain_flag, 0)) AS dest_drain,
     toInteger(coalesce(orig.transfer_cashout_flag, 0)) AS orig_transfer_cashout,
     toInteger(coalesce(dest.transfer_cashout_flag, 0)) AS dest_transfer_cashout,
     toInteger(coalesce(orig.dense_community_flag, 0)) AS orig_dense_community,
     toInteger(coalesce(dest.dense_community_flag, 0)) AS dest_dense_community,
     toInteger(coalesce(orig.cycles_flag, 0)) AS orig_cycles,
     toInteger(coalesce(dest.cycles_flag, 0)) AS dest_cycles,
     toInteger(coalesce(orig.association_flag, 0)) AS orig_association,
     toInteger(coalesce(dest.association_flag, 0)) AS dest_association,
     toInteger(coalesce(orig.ringtoring_flag, 0)) AS orig_ringtoring,
     toInteger(coalesce(dest.ringtoring_flag, 0)) AS dest_ringtoring
RETURN
  elementId(t) AS cursor,
  elementId(t) AS transaction_id,
  toString(orig.id) AS nameOrig,
  toString(dest.id) AS nameDest,
  toInteger(t.step) AS step,
  toString(t.type) AS type,
  amt AS amount,
  toFloat(t.oldbalanceOrg) AS oldbalanceOrg,
  toFloat(t.newbalanceOrig) AS newbalanceOrig,
  toFloat(t.oldbalanceDest) AS oldbalanceDest,
  toFloat(t.newbalanceDest) AS newbalanceDest,
  CASE WHEN t.isFraud IS NULL THEN null ELSE toInteger(t.isFraud) END AS is_fraud,
  CASE WHEN t.isFlaggedFraud IS NULL THEN null ELSE toInteger(t.isFlaggedFraud) END AS is_flagged_fraud,
  orig_comm AS orig_community_id,
  dest_comm AS dest_community_id,
  orig_risk AS orig_risk_score,
  dest_risk AS dest_risk_score,
  CASE WHEN orig_risk > 0 THEN 1 ELSE 0 END AS orig_is_flagged_account,
  CASE WHEN dest_risk > 0 THEN 1 ELSE 0 END AS dest_is_flagged_account,
  CASE WHEN orig_risk > 0 OR dest_risk > 0 THEN 1 ELSE 0 END AS has_flagged_endpoint,
  CASE WHEN orig_risk >= $high_risk_threshold OR dest_risk >= $high_risk_threshold THEN 1 ELSE 0 END AS has_high_risk_endpoint,
  CASE WHEN orig_risk > 0 OR dest_risk > 0 THEN amt ELSE 0.0 END AS flagged_endpoint_amount,
  CASE WHEN orig_risk >= $high_risk_threshold OR dest_risk >= $high_risk_threshold THEN amt ELSE 0.0 END AS high_risk_endpoint_amount,
  CASE WHEN orig_comm <> dest_comm THEN 1 ELSE 0 END AS cross_community_flag,
  CASE WHEN orig_fan_out > 0 OR dest_fan_out > 0 THEN 1 ELSE 0 END AS has_fan_out_endpoint,
  CASE WHEN orig_fan_in > 0 OR dest_fan_in > 0 THEN 1 ELSE 0 END AS has_fan_in_endpoint,
  CASE WHEN orig_drain > 0 OR dest_drain > 0 THEN 1 ELSE 0 END AS has_drain_endpoint,
  CASE WHEN orig_transfer_cashout > 0 OR dest_transfer_cashout > 0 THEN 1 ELSE 0 END AS has_transfer_cashout_endpoint,
  CASE WHEN orig_dense_community > 0 OR dest_dense_community > 0 THEN 1 ELSE 0 END AS has_dense_community_endpoint,
  CASE WHEN orig_cycles > 0 OR dest_cycles > 0 THEN 1 ELSE 0 END AS has_cycles_endpoint,
  CASE WHEN orig_association > 0 OR dest_association > 0 THEN 1 ELSE 0 END AS has_association_endpoint,
  CASE WHEN orig_ringtoring > 0 OR dest_ringtoring > 0 THEN 1 ELSE 0 END AS has_ringtoring_endpoint
ORDER BY elementId(t)
LIMIT $batch_size
"""

COMMUNITY_BASE_QUERY = """
MATCH (a:Account)
WITH coalesce(toString(a.community_id), 'UNASSIGNED') AS community_id,
     toFloat(coalesce(a.risk_score, 0.0)) AS risk_score,
     toInteger(coalesce(a.dense_community_flag, 0)) AS dense_flag
RETURN
  community_id,
  count(*) AS member_count,
  sum(CASE WHEN risk_score > 0 THEN 1 ELSE 0 END) AS flagged_member_count,
  sum(CASE WHEN risk_score >= $high_risk_threshold THEN 1 ELSE 0 END) AS high_risk_member_count,
  sum(CASE WHEN dense_flag > 0 THEN 1 ELSE 0 END) AS dense_community_member_count,
  avg(risk_score) AS avg_risk_score,
  max(risk_score) AS max_risk_score
ORDER BY community_id
"""

COMMUNITY_FLOW_QUERY = """
MATCH (src:Account)-[t:TRANSACTION]->(dst:Account)
WITH
  coalesce(toString(src.community_id), 'UNASSIGNED') AS from_community_id,
  coalesce(toString(dst.community_id), 'UNASSIGNED') AS to_community_id,
  toInteger(coalesce(src.dense_community_flag, 0)) AS src_dense_flag,
  toInteger(coalesce(dst.dense_community_flag, 0)) AS dst_dense_flag,
  toFloat(coalesce(src.risk_score, 0.0)) AS src_risk,
  toFloat(coalesce(dst.risk_score, 0.0)) AS dst_risk,
  toFloat(coalesce(t.amount, 0.0)) AS amount
RETURN
  from_community_id,
  to_community_id,
  CASE WHEN from_community_id = to_community_id THEN 1 ELSE 0 END AS is_internal_flow,
  CASE WHEN src_dense_flag > 0 AND dst_dense_flag > 0 THEN 1 ELSE 0 END AS both_suspicious_endpoint_members,
  count(*) AS tx_count,
  sum(amount) AS total_amount,
  sum(CASE WHEN src_risk > 0 OR dst_risk > 0 THEN amount ELSE 0.0 END) AS flagged_endpoint_amount
ORDER BY total_amount DESC
"""

# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def as_str(value, default=""):
    if value is None:
        return default
    return str(value)


def as_int(value, default=0):
    if value is None:
        return default
    return int(value)


def as_float(value, default=None):
    if value is None:
        return default
    return float(value)


def write_df(df, path, mode="overwrite", coalesce_to_one=False):
    writer_df = df.coalesce(1) if coalesce_to_one else df
    (
        writer_df.write
        .mode(mode)
        .option("compression", "snappy")
        .parquet(str(path))
    )


def make_spark():
    return (
        SparkSession.builder
        .appName("neo4j-paysim-pbi-parquet-export")
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "200"))
        .getOrCreate()
    )

# -----------------------------------------------------------------------------
# Dimensions and bridges
# -----------------------------------------------------------------------------
def write_dim_signal(spark, output_path):
    print("Writing dim_signal...")
    rows = [
        (
            s["signal_key"],
            s["signal_name"],
            s["flag_column"],
            s["transaction_column"],
            s["weight"],
            s["description"],
        )
        for s in SIGNALS
    ]
    df = spark.createDataFrame(rows, schema=DIM_SIGNAL_SCHEMA)
    write_df(df, output_path, coalesce_to_one=True)


def account_row_to_tuple(r):
    return (
        as_str(r["cursor"]),
        as_str(r["account_id"]),
        as_str(r["account_type"], "Other"),
        as_str(r["community_id"], "UNASSIGNED"),
        as_int(r["outdegree"], 0),
        as_int(r["fan_out_flag"], 0),
        as_int(r["fan_in_flag"], 0),
        as_int(r["drain_flag"], 0),
        as_int(r["transfer_cashout_flag"], 0),
        as_int(r["dense_community_flag"], 0),
        as_int(r["cycles_flag"], 0),
        as_int(r["association_flag"], 0),
        as_int(r["ringtoring_flag"], 0),
        as_float(r["risk_score"], 0.0),
        as_int(r["is_flagged_account"], 0),
        as_int(r["is_high_risk_account"], 0),
    )


def bridge_rows_from_account_tuple(row_tuple):
    # Indexes based on ACCOUNT_BATCH_SCHEMA order.
    account_id = row_tuple[1]
    by_name = {field.name: row_tuple[i] for i, field in enumerate(ACCOUNT_BATCH_SCHEMA.fields)}
    bridge_rows = []
    for signal in SIGNALS:
        flag_value = as_int(by_name.get(signal["flag_column"]), 0)
        if flag_value > 0:
            signal_weight = signal["weight"]
            bridge_rows.append((
                account_id,
                signal["signal_key"],
                signal["signal_name"],
                signal["flag_column"],
                flag_value,
                signal_weight,
                flag_value * signal_weight,
            ))
    return bridge_rows


def stream_accounts_and_bridge(driver, spark, dim_account_path, bridge_path):
    print(f"Streaming accounts in batches of {ACCOUNT_BATCH_SIZE}...")
    last_cursor = ""
    batch_num = 0
    total_rows = 0
    bridge_batches_written = 0

    while True:
        with driver.session() as session:
            result = session.run(
                ACCOUNTS_QUERY,
                last_cursor=last_cursor,
                batch_size=ACCOUNT_BATCH_SIZE,
                high_risk_threshold=HIGH_RISK_THRESHOLD,
            )
            batch = [account_row_to_tuple(r) for r in result]

        if not batch:
            break

        df = spark.createDataFrame(batch, schema=ACCOUNT_BATCH_SCHEMA).drop("cursor")
        write_mode = "overwrite" if batch_num == 0 else "append"
        write_df(df, dim_account_path, mode=write_mode)

        bridge_rows = []
        for row in batch:
            bridge_rows.extend(bridge_rows_from_account_tuple(row))

        if bridge_rows:
            bridge_df = spark.createDataFrame(bridge_rows, schema=BRIDGE_ACCOUNT_SIGNAL_SCHEMA)
            bridge_mode = "overwrite" if bridge_batches_written == 0 else "append"
            write_df(bridge_df, bridge_path, mode=bridge_mode)
            bridge_batches_written += 1

        last_cursor = batch[-1][0]
        batch_num += 1
        total_rows += len(batch)
        print(f"  accounts batch {batch_num}: {len(batch)} rows (total: {total_rows})")

    if batch_num == 0:
        empty_accounts = spark.createDataFrame([], schema=ACCOUNT_SCHEMA)
        write_df(empty_accounts, dim_account_path)

    if bridge_batches_written == 0:
        empty_bridge = spark.createDataFrame([], schema=BRIDGE_ACCOUNT_SIGNAL_SCHEMA)
        write_df(empty_bridge, bridge_path)

    print(f"Done streaming accounts. {total_rows} account rows written.")


def write_dim_transaction_type(spark, seen_types, output_path):
    print("Writing dim_transaction_type...")
    rows = [(i + 1, type_name) for i, type_name in enumerate(sorted(seen_types))]
    df = spark.createDataFrame(rows, schema=DIM_TYPE_SCHEMA)
    write_df(df, output_path, coalesce_to_one=True)


def write_dim_date(spark, max_step, output_path):
    print(f"Writing dim_date for {max_step} steps...")
    rows = []
    for step in range(1, max_step + 1):
        dt = START_DATE + timedelta(hours=step - 1)
        rows.append((
            step,
            dt.strftime("%Y-%m-%d %H:00:00"),
            dt.date(),
            dt.hour,
            ((step - 1) // 24) + 1,
            dt.strftime("%A"),
            ((step - 1) // (24 * 7)) + 1,
        ))
    df = spark.createDataFrame(rows, schema=DIM_DATE_SCHEMA)
    write_df(df, output_path, coalesce_to_one=True)

# -----------------------------------------------------------------------------
# Fact transaction streaming
# -----------------------------------------------------------------------------
def transaction_row_to_tuple(r):
    return (
        as_str(r["cursor"]),
        as_str(r["transaction_id"]),
        as_str(r["nameOrig"]),
        as_str(r["nameDest"]),
        as_int(r["step"]),
        as_str(r["type"]),
        as_float(r["amount"], 0.0),
        as_float(r["oldbalanceOrg"], None),
        as_float(r["newbalanceOrig"], None),
        as_float(r["oldbalanceDest"], None),
        as_float(r["newbalanceDest"], None),
        as_int(r["is_fraud"], None),
        as_int(r["is_flagged_fraud"], None),
        as_str(r["orig_community_id"], "UNASSIGNED"),
        as_str(r["dest_community_id"], "UNASSIGNED"),
        as_float(r["orig_risk_score"], 0.0),
        as_float(r["dest_risk_score"], 0.0),
        as_int(r["orig_is_flagged_account"], 0),
        as_int(r["dest_is_flagged_account"], 0),
        as_int(r["has_flagged_endpoint"], 0),
        as_int(r["has_high_risk_endpoint"], 0),
        as_float(r["flagged_endpoint_amount"], 0.0),
        as_float(r["high_risk_endpoint_amount"], 0.0),
        as_int(r["cross_community_flag"], 0),
        as_int(r["has_fan_out_endpoint"], 0),
        as_int(r["has_fan_in_endpoint"], 0),
        as_int(r["has_drain_endpoint"], 0),
        as_int(r["has_transfer_cashout_endpoint"], 0),
        as_int(r["has_dense_community_endpoint"], 0),
        as_int(r["has_cycles_endpoint"], 0),
        as_int(r["has_association_endpoint"], 0),
        as_int(r["has_ringtoring_endpoint"], 0),
    )


def stream_transactions_to_raw_parquet(driver, spark, output_path):
    print(f"Streaming transactions in batches of {BATCH_SIZE}...")
    last_cursor = ""
    batch_num = 0
    total_rows = 0
    seen_types = set()
    max_step = 0

    while True:
        with driver.session() as session:
            result = session.run(
                TRANSACTIONS_QUERY,
                last_cursor=last_cursor,
                batch_size=BATCH_SIZE,
                high_risk_threshold=HIGH_RISK_THRESHOLD,
            )
            batch = [transaction_row_to_tuple(r) for r in result]

        if not batch:
            break

        for row in batch:
            seen_types.add(row[5])
            if row[4] > max_step:
                max_step = row[4]

        df = spark.createDataFrame(batch, schema=TRANSACTION_BATCH_SCHEMA).drop("cursor")
        write_mode = "overwrite" if batch_num == 0 else "append"
        write_df(df, output_path, mode=write_mode)

        last_cursor = batch[-1][0]
        batch_num += 1
        total_rows += len(batch)
        print(f"  transactions batch {batch_num}: {len(batch)} rows (total: {total_rows})")

    if batch_num == 0:
        empty_fact = spark.createDataFrame([], schema=TRANSACTION_SCHEMA)
        write_df(empty_fact, output_path)

    print(f"Done streaming transactions. {total_rows} transaction rows written.")
    return seen_types, max_step


def finalize_fact_transactions(spark, raw_path, dim_date_path, dim_type_path, final_path):
    print("Finalizing fact_transactions with date/type keys and date partitioning...")
    fact = spark.read.parquet(str(raw_path))
    dim_date = spark.read.parquet(str(dim_date_path))
    dim_type = spark.read.parquet(str(dim_type_path))

    fact_with_date = fact.join(
        dim_date.select(F.col("date_key").alias("step"), "date"),
        on="step",
        how="inner",
    )
    fact_final = fact_with_date.join(
        dim_type.select("type_key", F.col("type_name").alias("type")),
        on="type",
        how="left",
    )

    (
        fact_final.write
        .mode("overwrite")
        .option("compression", "snappy")
        .partitionBy("date")
        .parquet(str(final_path))
    )

# -----------------------------------------------------------------------------
# Communities/rings
# -----------------------------------------------------------------------------
def fetch_community_base(driver):
    print("Fetching community account summary...")
    with driver.session() as session:
        result = session.run(COMMUNITY_BASE_QUERY, high_risk_threshold=HIGH_RISK_THRESHOLD)
        rows = [
            (
                as_str(r["community_id"], "UNASSIGNED"),
                as_int(r["member_count"], 0),
                as_int(r["flagged_member_count"], 0),
                as_int(r["high_risk_member_count"], 0),
                as_int(r["dense_community_member_count"], 0),
                as_float(r["avg_risk_score"], 0.0),
                as_float(r["max_risk_score"], 0.0),
            )
            for r in result
        ]
    print(f"  Got {len(rows)} communities")
    return rows


def fetch_community_flow(driver):
    print("Fetching community flow summary...")
    with driver.session() as session:
        result = session.run(COMMUNITY_FLOW_QUERY)
        rows = [
            (
                as_str(r["from_community_id"], "UNASSIGNED"),
                as_str(r["to_community_id"], "UNASSIGNED"),
                as_int(r["is_internal_flow"], 0),
                as_int(r["both_suspicious_endpoint_members"], 0),
                as_int(r["tx_count"], 0),
                as_float(r["total_amount"], 0.0),
                as_float(r["flagged_endpoint_amount"], 0.0),
            )
            for r in result
        ]
    print(f"  Got {len(rows)} community-flow rows")
    return rows


def write_fact_community_flow(spark, rows, output_path):
    print("Writing fact_community_flow...")
    df = spark.createDataFrame(rows, schema=COMMUNITY_FLOW_SCHEMA)
    write_df(df, output_path)


def write_dim_community(spark, community_base_rows, fact_community_flow_path, output_path):
    print("Writing dim_community...")
    base = spark.createDataFrame(community_base_rows, schema=COMMUNITY_BASE_SCHEMA)
    flow = spark.read.parquet(str(fact_community_flow_path))

    internal = (
        flow.filter(F.col("is_internal_flow") == 1)
        .groupBy(F.col("from_community_id").alias("community_id"))
        .agg(
            F.sum("total_amount").alias("internal_volume"),
            F.sum("tx_count").alias("internal_tx_count"),
        )
    )
    outgoing = (
        flow.filter(F.col("is_internal_flow") == 0)
        .groupBy(F.col("from_community_id").alias("community_id"))
        .agg(
            F.sum("total_amount").alias("outgoing_volume"),
            F.sum("tx_count").alias("outgoing_tx_count"),
        )
    )
    incoming = (
        flow.filter(F.col("is_internal_flow") == 0)
        .groupBy(F.col("to_community_id").alias("community_id"))
        .agg(
            F.sum("total_amount").alias("incoming_volume"),
            F.sum("tx_count").alias("incoming_tx_count"),
        )
    )

    dim = (
        base
        .join(internal, on="community_id", how="left")
        .join(outgoing, on="community_id", how="left")
        .join(incoming, on="community_id", how="left")
        .fillna({
            "internal_volume": 0.0,
            "internal_tx_count": 0,
            "outgoing_volume": 0.0,
            "outgoing_tx_count": 0,
            "incoming_volume": 0.0,
            "incoming_tx_count": 0,
        })
        .withColumn("internal_to_outgoing_ratio", F.round(F.col("internal_volume") / (F.col("outgoing_volume") + F.lit(1.0)), 4))
        .withColumn("is_suspicious_ring", F.when(F.col("dense_community_member_count") > 0, F.lit(1)).otherwise(F.lit(0)))
        .withColumn("ring_label", F.when(F.col("dense_community_member_count") > 0, F.lit("Suspicious ring")).otherwise(F.lit("Normal / unflagged community")))
    )
    write_df(dim, output_path)

# -----------------------------------------------------------------------------
# Summary fact tables for PowerBI convenience
# -----------------------------------------------------------------------------
def write_risk_distribution(spark, dim_account_path, output_path):
    print("Writing fact_risk_score_distribution...")
    accounts = spark.read.parquet(str(dim_account_path))
    df = (
        accounts
        .groupBy("risk_score")
        .agg(F.count("*").alias("account_count"))
        .orderBy("risk_score")
    )
    write_df(df, output_path, coalesce_to_one=True)


def write_signal_transaction_summary(spark, fact_transactions_path, output_path):
    print("Writing fact_signal_transaction_summary...")
    fact = spark.read.parquet(str(fact_transactions_path))
    rows = []
    for signal in SIGNALS:
        tx_col = signal["transaction_column"]
        agg_row = fact.agg(
            F.sum(F.col(tx_col)).alias("transaction_count"),
            F.sum(F.when(F.col(tx_col) == 1, F.col("amount")).otherwise(F.lit(0.0))).alias("total_amount"),
            F.sum(F.when(F.col(tx_col) == 1, F.col("flagged_endpoint_amount")).otherwise(F.lit(0.0))).alias("flagged_endpoint_amount"),
        ).collect()[0]
        rows.append((
            signal["signal_key"],
            signal["signal_name"],
            signal["flag_column"],
            tx_col,
            as_int(agg_row["transaction_count"], 0),
            as_float(agg_row["total_amount"], 0.0),
            as_float(agg_row["flagged_endpoint_amount"], 0.0),
        ))

    schema = StructType([
        StructField("signal_key", IntegerType(), False),
        StructField("signal_name", StringType(), False),
        StructField("flag_column", StringType(), False),
        StructField("transaction_column", StringType(), False),
        StructField("transaction_count", LongType(), False),
        StructField("total_amount", DoubleType(), False),
        StructField("flagged_endpoint_amount", DoubleType(), False),
    ])
    df = spark.createDataFrame(rows, schema=schema)
    write_df(df, output_path, coalesce_to_one=True)


def write_dashboard_kpis(spark, dim_account_path, dim_community_path, fact_transactions_path, output_path):
    print("Writing fact_dashboard_kpis...")
    accounts = spark.read.parquet(str(dim_account_path))
    communities = spark.read.parquet(str(dim_community_path))
    fact = spark.read.parquet(str(fact_transactions_path))

    account_row = accounts.agg(
        F.count("*").alias("total_accounts"),
        F.sum("is_flagged_account").alias("flagged_accounts"),
        F.sum("is_high_risk_account").alias("high_risk_accounts"),
        F.max("risk_score").alias("max_risk_score"),
    ).collect()[0]

    community_row = communities.agg(
        F.count("*").alias("total_communities"),
        F.sum("is_suspicious_ring").alias("rings_detected"),
        F.max("internal_volume").alias("max_internal_ring_volume"),
    ).collect()[0]

    fact_row = fact.agg(
        F.count("*").alias("total_transactions"),
        F.sum("amount").alias("total_transaction_amount"),
        F.sum("flagged_endpoint_amount").alias("total_flagged_endpoint_volume"),
        F.sum("high_risk_endpoint_amount").alias("total_high_risk_endpoint_volume"),
        F.sum("has_flagged_endpoint").alias("transactions_with_flagged_endpoint"),
        F.sum("has_high_risk_endpoint").alias("transactions_with_high_risk_endpoint"),
        F.sum(F.coalesce(F.col("is_fraud"), F.lit(0))).alias("paysim_fraud_transaction_count"),
    ).collect()[0]

    rows = [(
        as_int(account_row["total_accounts"], 0),
        as_int(account_row["flagged_accounts"], 0),
        as_int(account_row["high_risk_accounts"], 0),
        as_float(account_row["max_risk_score"], 0.0),
        as_int(community_row["total_communities"], 0),
        as_int(community_row["rings_detected"], 0),
        as_float(community_row["max_internal_ring_volume"], 0.0),
        as_int(fact_row["total_transactions"], 0),
        as_float(fact_row["total_transaction_amount"], 0.0),
        as_float(fact_row["total_flagged_endpoint_volume"], 0.0),
        as_float(fact_row["total_high_risk_endpoint_volume"], 0.0),
        as_int(fact_row["transactions_with_flagged_endpoint"], 0),
        as_int(fact_row["transactions_with_high_risk_endpoint"], 0),
        as_int(fact_row["paysim_fraud_transaction_count"], 0),
        HIGH_RISK_THRESHOLD,
    )]

    schema = StructType([
        StructField("total_accounts", LongType(), False),
        StructField("flagged_accounts", LongType(), False),
        StructField("high_risk_accounts", LongType(), False),
        StructField("max_risk_score", DoubleType(), False),
        StructField("total_communities", LongType(), False),
        StructField("rings_detected", LongType(), False),
        StructField("max_internal_ring_volume", DoubleType(), False),
        StructField("total_transactions", LongType(), False),
        StructField("total_transaction_amount", DoubleType(), False),
        StructField("total_flagged_endpoint_volume", DoubleType(), False),
        StructField("total_high_risk_endpoint_volume", DoubleType(), False),
        StructField("transactions_with_flagged_endpoint", LongType(), False),
        StructField("transactions_with_high_risk_endpoint", LongType(), False),
        StructField("paysim_fraud_transaction_count", LongType(), False),
        StructField("high_risk_threshold_used", DoubleType(), False),
    ])
    df = spark.createDataFrame(rows, schema=schema)
    write_df(df, output_path, coalesce_to_one=True)

# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    print(f"Connecting to Neo4j at {NEO4J_URI}...")
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

    print("Starting Spark session...")
    spark = make_spark()

    # Output paths.
    dim_account_path = OUTPUT_DIR / "dim_account"
    dim_type_path = OUTPUT_DIR / "dim_transaction_type"
    dim_date_path = OUTPUT_DIR / "dim_date"
    dim_signal_path = OUTPUT_DIR / "dim_signal"
    bridge_account_signal_path = OUTPUT_DIR / "bridge_account_signal"
    dim_community_path = OUTPUT_DIR / "dim_community"
    fact_raw_path = OUTPUT_DIR / "_fact_transactions_raw"
    fact_final_path = OUTPUT_DIR / "fact_transactions"
    fact_community_flow_path = OUTPUT_DIR / "fact_community_flow"
    fact_kpis_path = OUTPUT_DIR / "fact_dashboard_kpis"
    fact_risk_dist_path = OUTPUT_DIR / "fact_risk_score_distribution"
    fact_signal_summary_path = OUTPUT_DIR / "fact_signal_transaction_summary"

    try:
        # Clean only intermediate raw output first. Final table folders are overwritten individually.
        shutil.rmtree(fact_raw_path, ignore_errors=True)

        write_dim_signal(spark, dim_signal_path)

        # Account dimension and account-signal bridge.
        stream_accounts_and_bridge(driver, spark, dim_account_path, bridge_account_signal_path)

        # Community/ring aggregates from graph.
        community_base_rows = fetch_community_base(driver)
        community_flow_rows = fetch_community_flow(driver)
        write_fact_community_flow(spark, community_flow_rows, fact_community_flow_path)
        write_dim_community(spark, community_base_rows, fact_community_flow_path, dim_community_path)

        # Transaction fact table.
        seen_types, max_step = stream_transactions_to_raw_parquet(driver, spark, fact_raw_path)

        if not seen_types:
            print("WARNING: No transaction types were found. Check graph labels and TRANSACTION relationship type.")
        if max_step <= 0:
            print("WARNING: max_step was 0. dim_date will be empty.")

        write_dim_transaction_type(spark, seen_types, dim_type_path)
        write_dim_date(spark, max_step, dim_date_path)
        finalize_fact_transactions(spark, fact_raw_path, dim_date_path, dim_type_path, fact_final_path)

        # Dashboard helper tables.
        write_risk_distribution(spark, dim_account_path, fact_risk_dist_path)
        write_signal_transaction_summary(spark, fact_final_path, fact_signal_summary_path)
        write_dashboard_kpis(spark, dim_account_path, dim_community_path, fact_final_path, fact_kpis_path)

        print("Cleaning up intermediate transaction files...")
        shutil.rmtree(fact_raw_path, ignore_errors=True)

        print("\nDone. PowerBI-ready Parquet output written to:")
        print(f"  {OUTPUT_DIR.resolve()}")
        print("\nLoad each folder above as a separate table in PowerBI.")

    finally:
        driver.close()
        spark.stop()


if __name__ == "__main__":
    main()
