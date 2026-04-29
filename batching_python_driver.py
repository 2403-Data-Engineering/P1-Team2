"""
batching_python_driver.py

Neo4j PaySim graph -> minimal trainer-style Parquet star schema for PowerBI.

Final exported tables/folders:
  - fact_transactions
  - dim_account
  - dim_transaction_type
  - dim_date

This version keeps the trainer's table count small, but it batches BOTH:
  - dim_account
  - fact_transactions

Why this patched version exists:
  Full PaySim-style graphs can contain millions of Account nodes. Pulling all
  accounts into one Python list and then calling spark.createDataFrame(...) can
  crash with Java heap space errors. This version writes dim_account in batches
  exactly like fact_transactions, so memory stays bounded.

Run:
  python batching_python_driver.py

Optional environment variables:
  NEO4J_URI=bolt://localhost:7687
  NEO4J_USER=neo4j
  NEO4J_PASSWORD=password
  OUTPUT_DIR=./Output_Parquet
  START_DATE=2026-03-01
  BATCH_SIZE=25000
  ACCOUNT_BATCH_SIZE=25000
  SPARK_DRIVER_MEMORY=4g
"""

import os
import sys
import shutil
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Optional

# Spark needs to know which Python executable to use, especially on Windows.
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from neo4j import GraphDatabase
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
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
NEO4J_DATABASE = os.getenv("NEO4J_DATABASE")  # optional; leave unset for default DB

# PaySim step = hour number. This start date is artificial and only exists so
# PowerBI can use normal date slicers/axes.
START_DATE = datetime.strptime(os.getenv("START_DATE", "2026-03-01"), "%Y-%m-%d")

OUTPUT_DIR = Path(os.getenv("OUTPUT_DIR", "./Output_Parquet"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "25000"))
ACCOUNT_BATCH_SIZE = int(os.getenv("ACCOUNT_BATCH_SIZE", str(BATCH_SIZE)))
SPARK_DRIVER_MEMORY = os.getenv("SPARK_DRIVER_MEMORY", "4g")

# -----------------------------------------------------------------------------
# Small conversion helpers
# -----------------------------------------------------------------------------
def safe_int(value: Any, default: int = 0) -> int:
    if value is None:
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def safe_float(value: Any, default: Optional[float] = None) -> Optional[float]:
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def safe_str(value: Any, default: str = "") -> str:
    if value is None:
        return default
    return str(value)


def session_kwargs() -> dict:
    """Pass database only when supplied, so this works with default Neo4j setups."""
    return {"database": NEO4J_DATABASE} if NEO4J_DATABASE else {}

# -----------------------------------------------------------------------------
# Schemas — keep in sync with the Cypher RETURN clauses and row tuples.
# -----------------------------------------------------------------------------
ACCOUNT_BATCH_SCHEMA = StructType([
    StructField("account_id", StringType(), False),
    StructField("community_id", StringType(), True),
    StructField("indegree", IntegerType(), True),
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
])

# Cursor is used only for pagination and is dropped before writing Parquet.
TRANSACTION_BATCH_SCHEMA = StructType([
    StructField("cursor", StringType(), False),
    StructField("nameOrig", StringType(), False),
    StructField("nameDest", StringType(), False),
    StructField("step", IntegerType(), False),
    StructField("type", StringType(), False),
    StructField("amount", DoubleType(), False),
    StructField("oldbalanceOrg", DoubleType(), True),
    StructField("newbalanceOrig", DoubleType(), True),
    StructField("oldbalanceDest", DoubleType(), True),
    StructField("newbalanceDest", DoubleType(), True),
])

DIM_TYPE_SCHEMA = StructType([
    StructField("type_key", IntegerType(), False),
    StructField("type_name", StringType(), False),
])

DIM_DATE_SCHEMA = StructType([
    StructField("date_key", IntegerType(), False),
    StructField("datetime", StringType(), False),
    StructField("date", DateType(), False),
    StructField("hour", IntegerType(), False),
    StructField("day_of_week", StringType(), False),
])

# -----------------------------------------------------------------------------
# Cypher queries
# -----------------------------------------------------------------------------
# Stream Account nodes with a single query and Neo4j driver fetch_size.
# Do NOT ORDER BY here; sorting millions of accounts is slow and unnecessary.
ACCOUNTS_QUERY = """
MATCH (a:Account)
RETURN
  a.id AS account_id,
  toString(a.community_id) AS community_id,
  a.indegree AS indegree,
  a.outdegree AS outdegree,
  coalesce(a.fan_out_flag, 0) AS fan_out_flag,
  coalesce(a.fan_in_flag, 0) AS fan_in_flag,
  coalesce(a.drain_flag, 0) AS drain_flag,
  coalesce(a.transfer_cashout_flag, 0) AS transfer_cashout_flag,
  coalesce(a.dense_community_flag, 0) AS dense_community_flag,
  coalesce(a.cycles_flag, 0) AS cycles_flag,
  coalesce(a.association_flag, 0) AS association_flag,
  coalesce(a.ringtoring_flag, 0) AS ringtoring_flag,
  coalesce(a.risk_score, 0.0) AS risk_score
"""

TRANSACTIONS_QUERY = """
MATCH (orig:Account)-[t:TRANSACTION]->(dest:Account)
WHERE elementId(t) > $last_cursor
RETURN
  elementId(t) AS cursor,
  orig.id AS nameOrig,
  dest.id AS nameDest,
  t.step AS step,
  t.type AS type,
  t.amount AS amount,
  t.oldbalanceOrg AS oldbalanceOrg,
  t.newbalanceOrig AS newbalanceOrig,
  t.oldbalanceDest AS oldbalanceDest,
  t.newbalanceDest AS newbalanceDest
ORDER BY elementId(t)
LIMIT $batch_size
"""

# -----------------------------------------------------------------------------
# Read/write functions
# -----------------------------------------------------------------------------
def stream_accounts_to_parquet(driver, spark, output_path):
    """
    Read Account nodes through the Neo4j driver's streaming result and write
    each ACCOUNT_BATCH_SIZE chunk to dim_account. This avoids materializing
    millions of accounts in one Python list and avoids a huge Spark driver heap
    serialization step.
    """
    batch_num = 0
    total_rows = 0
    batch = []

    print(f"Streaming accounts for dim_account in batches of {ACCOUNT_BATCH_SIZE}...")

    with driver.session(fetch_size=ACCOUNT_BATCH_SIZE, **session_kwargs()) as session:
        result = session.run(ACCOUNTS_QUERY)

        for r in result:
            batch.append((
                safe_str(r["account_id"]),
                safe_str(r["community_id"]),
                safe_int(r["indegree"]),
                safe_int(r["outdegree"]),
                safe_int(r["fan_out_flag"]),
                safe_int(r["fan_in_flag"]),
                safe_int(r["drain_flag"]),
                safe_int(r["transfer_cashout_flag"]),
                safe_int(r["dense_community_flag"]),
                safe_int(r["cycles_flag"]),
                safe_int(r["association_flag"]),
                safe_int(r["ringtoring_flag"]),
                safe_float(r["risk_score"], 0.0),
            ))

            if len(batch) >= ACCOUNT_BATCH_SIZE:
                df = spark.createDataFrame(batch, schema=ACCOUNT_BATCH_SCHEMA)
                write_mode = "overwrite" if batch_num == 0 else "append"
                (
                    df.write.mode(write_mode)
                    .option("compression", "snappy")
                    .parquet(str(output_path))
                )
                batch_num += 1
                total_rows += len(batch)
                print(f"  account batch {batch_num}: {len(batch)} rows (total: {total_rows})")
                batch = []

        if batch:
            df = spark.createDataFrame(batch, schema=ACCOUNT_BATCH_SCHEMA)
            write_mode = "overwrite" if batch_num == 0 else "append"
            (
                df.write.mode(write_mode)
                .option("compression", "snappy")
                .parquet(str(output_path))
            )
            batch_num += 1
            total_rows += len(batch)
            print(f"  account batch {batch_num}: {len(batch)} rows (total: {total_rows})")

    print(f"Done streaming dim_account. {total_rows} accounts written across {batch_num} batches.")


def stream_transactions_to_parquet(driver, spark, output_path):
    """
    Read transactions in keyset-paginated batches and append each batch to an
    intermediate Parquet folder. Returns transaction types and max step so the
    two small dimensions can be created after streaming.
    """
    last_cursor = ""
    batch_num = 0
    total_rows = 0
    seen_types = set()
    max_step = 0

    print(f"Streaming transactions in batches of {BATCH_SIZE}...")

    while True:
        with driver.session(**session_kwargs()) as session:
            result = session.run(
                TRANSACTIONS_QUERY,
                last_cursor=last_cursor,
                batch_size=BATCH_SIZE,
            )
            batch = [
                (
                    safe_str(r["cursor"]),
                    safe_str(r["nameOrig"]),
                    safe_str(r["nameDest"]),
                    safe_int(r["step"]),
                    safe_str(r["type"]),
                    safe_float(r["amount"], 0.0),
                    safe_float(r["oldbalanceOrg"]),
                    safe_float(r["newbalanceOrig"]),
                    safe_float(r["oldbalanceDest"]),
                    safe_float(r["newbalanceDest"]),
                )
                for r in result
            ]

        if not batch:
            break

        for row in batch:
            seen_types.add(row[4])
            max_step = max(max_step, row[3])

        df = spark.createDataFrame(batch, schema=TRANSACTION_BATCH_SCHEMA).drop("cursor")
        write_mode = "overwrite" if batch_num == 0 else "append"
        (
            df.write.mode(write_mode)
            .option("compression", "snappy")
            .parquet(str(output_path))
        )

        last_cursor = batch[-1][0]
        batch_num += 1
        total_rows += len(batch)
        print(f"  transaction batch {batch_num}: {len(batch)} rows (total: {total_rows})")

    print(f"Done streaming. {total_rows} transactions written across {batch_num} batches.")
    return seen_types, max_step


def write_dim_transaction_type(spark, seen_types, output_path):
    print("Writing dim_transaction_type...")
    rows = [(i + 1, name) for i, name in enumerate(sorted(t for t in seen_types if t))]
    df = spark.createDataFrame(rows, schema=DIM_TYPE_SCHEMA)
    (
        df.coalesce(1)
        .write.mode("overwrite")
        .option("compression", "snappy")
        .parquet(str(output_path))
    )


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
            dt.strftime("%A"),
        ))

    df = spark.createDataFrame(rows, schema=DIM_DATE_SCHEMA)
    (
        df.coalesce(1)
        .write.mode("overwrite")
        .option("compression", "snappy")
        .parquet(str(output_path))
    )


def repartition_fact_by_date(spark, raw_path, dim_date_path, final_path):
    print("Repartitioning fact_transactions by date...")
    fact = spark.read.parquet(str(raw_path))
    dim_date = spark.read.parquet(str(dim_date_path))

    fact_with_date = fact.join(
        dim_date.select(dim_date.date_key.alias("step"), "date"),
        on="step",
        how="inner",
    )

    (
        fact_with_date.write.mode("overwrite")
        .option("compression", "snappy")
        .partitionBy("date")
        .parquet(str(final_path))
    )

# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    print(f"Connecting to {NEO4J_URI}...")
    if NEO4J_DATABASE:
        print(f"Using Neo4j database: {NEO4J_DATABASE}")
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

    print("Starting Spark session...")
    spark = (
        SparkSession.builder
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        .appName("neo4j-to-parquet-minimal-star-schema-batched-accounts")
        .config("spark.driver.memory", SPARK_DRIVER_MEMORY)
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SQL_SHUFFLE_PARTITIONS", "8"))
        .getOrCreate()
    )

    dim_account_path = OUTPUT_DIR / "dim_account"
    dim_type_path = OUTPUT_DIR / "dim_transaction_type"
    dim_date_path = OUTPUT_DIR / "dim_date"
    fact_raw_path = OUTPUT_DIR / "_fact_transactions_raw"
    fact_final_path = OUTPUT_DIR / "fact_transactions"

    try:
        stream_accounts_to_parquet(driver, spark, dim_account_path)

        seen_types, max_step = stream_transactions_to_parquet(driver, spark, fact_raw_path)

        write_dim_transaction_type(spark, seen_types, dim_type_path)
        write_dim_date(spark, max_step, dim_date_path)
        repartition_fact_by_date(spark, fact_raw_path, dim_date_path, fact_final_path)

        print("Cleaning up intermediate files...")
        shutil.rmtree(fact_raw_path, ignore_errors=True)

        print(f"\nDone. Output: {OUTPUT_DIR.resolve()}")
        print("Final folders: dim_account, dim_transaction_type, dim_date, fact_transactions")

    finally:
        try:
            driver.close()
        except Exception:
            pass
        spark.stop()


if __name__ == "__main__":
    main()
