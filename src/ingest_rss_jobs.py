import feedparser
from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

from ingest_log import start_run, finish_run, write_run_log

spark = SparkSession.builder.getOrCreate()

FEEDS = [
    "https://stackoverflow.com/jobs/feed",
    "https://weworkremotely.com/categories/remote-programming-jobs.rss"
]

target_table = "jobmarket.bronze.rss_jobs_raw"

run = start_run("rss_jobs")

try:
    run_ts = datetime.now(timezone.utc).isoformat()
    run_date = datetime.now(timezone.utc).date().isoformat()

    print(f"📡 RSS ingestion starting {run_ts}")

    rows = []
    for feed_url in FEEDS:
        feed = feedparser.parse(feed_url)
        for entry in feed.entries:
            rows.append({
                "source": feed_url,
                "title": entry.get("title"),
                "link": entry.get("link"),
                "published": entry.get("published"),
                "summary": entry.get("summary"),
                "run_ts": run_ts,
                "run_date": run_date
            })

    print(f"Collected {len(rows)} RSS entries")

    schema = StructType([
        StructField("source", StringType(), True),
        StructField("title", StringType(), True),
        StructField("link", StringType(), True),
        StructField("published", StringType(), True),
        StructField("summary", StringType(), True),
        StructField("run_ts", StringType(), True),
        StructField("run_date", StringType(), True),
    ])

    df = spark.createDataFrame(rows, schema)

    (df.write
       .format("delta")
       .mode("append")
       .partitionBy("run_date")
       .saveAsTable(target_table))

    written = df.count()
    run = finish_run(run, "SUCCESS", written, {"feeds": len(FEEDS), "target_table": target_table})
    write_run_log(run)

    print(f"✅ Wrote {written} RSS rows")
    df.show(10, truncate=False)

except Exception as e:
    run = finish_run(run, "FAILED", 0, {"error": str(e)})
    write_run_log(run)
    raise