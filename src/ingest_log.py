import json
import time
from datetime import datetime, timezone
from pyspark.sql import SparkSession

def start_run(source: str) -> dict:
    now = datetime.now(timezone.utc)
    return {
        "source": source,
        "run_ts": now.isoformat(),
        "run_date": now.date().isoformat(),
        "t0": time.time(),
        "status": "RUNNING",
        "rows_written": 0,
        "duration_seconds": 0.0,
        "details": "{}",
    }

def finish_run(run: dict, status: str, rows_written: int, details: dict | None = None) -> dict:
    run["status"] = status
    run["rows_written"] = int(rows_written)
    run["duration_seconds"] = float(time.time() - run["t0"])
    if details is not None:
        run["details"] = json.dumps(details, ensure_ascii=False)
    return run

def write_run_log(run: dict, table: str = "jobmarket.bronze.ingest_runs"):
    spark = SparkSession.builder.getOrCreate()
    # drop internal timer field
    row = {k: v for k, v in run.items() if k != "t0"}
    df = spark.createDataFrame([row])
    (df.write.format("delta")
       .mode("append")
       .partitionBy("run_date")
       .saveAsTable(table))