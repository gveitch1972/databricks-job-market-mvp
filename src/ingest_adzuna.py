import json
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, to_timestamp

from ingest_log import start_run, finish_run, write_run_log

import argparse
# ...
parser = argparse.ArgumentParser()
parser.add_argument("--bronze-table", default="jobmarket.bronze.adzuna_jobs_raw")
args = parser.parse_args()
target_table = args.bronze_table


def _get_secret(scope: str, key: str) -> Optional[str]:
    """
    Read a Databricks secret if dbutils is available.
    Returns None if not running on Databricks or secret not available.
    """
    try:
        # Databricks injects dbutils in notebooks; for Jobs it’s typically present too.
        # If it isn’t, we fall back to env vars.
        dbutils  # type: ignore[name-defined]  # noqa: F821
        return dbutils.secrets.get(scope, key)  # type: ignore[name-defined]  # noqa: F821
    except Exception:
        return None


def get_adzuna_creds() -> Dict[str, str]:
    # Prefer secrets
    app_id = _get_secret("job-market", "adzuna_app_id")
    app_key = _get_secret("job-market", "adzuna_app_key")

    # Fallback to env vars (handy for local testing)
    app_id = app_id or os.getenv("ADZUNA_APP_ID")
    app_key = app_key or os.getenv("ADZUNA_APP_KEY")

    if not app_id or not app_key:
        raise RuntimeError(
            "Missing Adzuna credentials. Set secrets job-market/adzuna_app_id and job-market/adzuna_app_key "
            "or export ADZUNA_APP_ID / ADZUNA_APP_KEY."
        )
    return {"app_id": app_id, "app_key": app_key}


def fetch_adzuna_page(
    *, app_id: str, app_key: str, country: str, page: int, results_per_page: int, what: str, where: str
) -> Dict[str, Any]:
    url = f"https://api.adzuna.com/v1/api/jobs/{country}/search/{page}"
    params = {
        "app_id": app_id,
        "app_key": app_key,
        "results_per_page": results_per_page,
        "what": what,
        "where": where,
        "content-type": "application/json",
    }
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    return r.json()


def main():
    spark = SparkSession.builder.getOrCreate()
    
    run = start_run("adzuna_jobs")

    try:

        run_ts = datetime.now(timezone.utc).isoformat()
        run_date = datetime.now(timezone.utc).date().isoformat()

        print(f"🚀 Adzuna ingestion starting. run_ts={run_ts} run_date={run_date}")

        creds = get_adzuna_creds()
        app_id, app_key = creds["app_id"], creds["app_key"]

        # MVP controls (keep small first)
        country = os.getenv("ADZUNA_COUNTRY", "gb")
        what = os.getenv("ADZUNA_WHAT", "data engineer")
        where = os.getenv("ADZUNA_WHERE", "Edinburgh")
        pages = int(os.getenv("ADZUNA_PAGES", "10"))
        results_per_page = int(os.getenv("ADZUNA_RESULTS_PER_PAGE", "50"))

        all_results: List[Dict[str, Any]] = []
        for page in range(1, pages + 1):
            payload = fetch_adzuna_page(
                app_id=app_id,
                app_key=app_key,
                country=country,
                page=page,
                results_per_page=results_per_page,
                what=what,
                where=where,
            )
            results = payload.get("results", []) or []

            print(f"Fetched page {page}/{pages}: {len(results)} results (total so far {len(all_results)})")

            # ✅ STOP if no results returned
            if len(results) == 0:
                print("No more results returned — stopping pagination early.")
                break

            all_results.extend(results)
            time.sleep(0.5)  # be polite

        # Store both raw JSON and a few extracted fields for quick querying
        rows = []
        for r in all_results:
            rows.append(
                {
                    "adzuna_id": r.get("id"),
                    "title": r.get("title"),
                    "company": (r.get("company") or {}).get("display_name") if isinstance(r.get("company"), dict) else None,
                    "location": (r.get("location") or {}).get("display_name")
                    if isinstance(r.get("location"), dict)
                    else None,
                    "created": r.get("created"),
                    "category": (r.get("category") or {}).get("label") if isinstance(r.get("category"), dict) else None,
                    "salary_min": float(r.get("salary_min")) if r.get("salary_min") else None,
                    "salary_max": float(r.get("salary_max")) if r.get("salary_max") else None,
                    "contract_time": r.get("contract_time"),
                    "contract_type": r.get("contract_type"),
                    "redirect_url": r.get("redirect_url"),
                    "raw_json": json.dumps(r, ensure_ascii=False),
                    "run_ts": run_ts,
                    "run_date": run_date,
                    "query_what": what,
                    "query_where": where,
                    "country": country,
                }
            )

        from pyspark.sql.types import (
            StructType,
            StructField,
            StringType,
            DoubleType,
        )

        schema = StructType([
            StructField("adzuna_id", StringType(), True),
            StructField("title", StringType(), True),
            StructField("company", StringType(), True),
            StructField("location", StringType(), True),
            StructField("created", StringType(), True),
            StructField("category", StringType(), True),
            StructField("salary_min", DoubleType(), True),
            StructField("salary_max", DoubleType(), True),
            StructField("contract_time", StringType(), True),
            StructField("contract_type", StringType(), True),
            StructField("redirect_url", StringType(), True),
            StructField("raw_json", StringType(), True),
            StructField("run_ts", StringType(), True),
            StructField("run_date", StringType(), True),
            StructField("query_what", StringType(), True),
            StructField("query_where", StringType(), True),
            StructField("country", StringType(), True),
        ])

        df = spark.createDataFrame(rows, schema=schema)

        # Parse created timestamp if present
        df = df.withColumn("created_ts", to_timestamp(col("created")))

        # Choose table name: UC vs non-UC
        #target_table = os.getenv("BRONZE_TABLE", "jobmarket.bronze.adzuna_jobs_raw")
        # Example if using UC: export BRONZE_TABLE="main.bronze.adzuna_jobs_raw"

        from delta.tables import DeltaTable

        # Create table if missing (empty with schema)
        try:
            DeltaTable.forName(spark, target_table)
        except Exception:
            (df.limit(0)
            .write.format("delta")
            .mode("overwrite")
            .saveAsTable(target_table))
            print(f"Created empty Delta table {target_table}")

        delta_table = DeltaTable.forName(spark, target_table)

        (delta_table.alias("t")
        .merge(df.alias("s"), "t.adzuna_id = s.adzuna_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())

        print(f"✅ Upserted {df.count()} rows into {target_table}")
        df.select("adzuna_id", "title", "company", "location", "created_ts", "run_date").show(10, truncate=False)

        written = df.count()
        run = finish_run(run, "SUCCESS", written, {
            "country": country,
            "what": what,
            "where": where,
            "pages_requested": pages,
            "results_per_page": results_per_page,
            "target_table": target_table
        })
        write_run_log(run)

    except Exception as e:
        run = finish_run(run, "FAILED", 0, {"error": str(e), "target_table": target_table})
        write_run_log(run)
        raise

if __name__ == "__main__":
    main()
