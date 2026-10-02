"""Step 2: Spark Structured Streaming over the recorded events, file by file, as if they were arriving live.

    ./venv/bin/python stream.py     -> data/out/{per_minute,per_wiki,bursts}/ (Parquet), results/stream_progress.csv

Three streaming queries share one source (3 files per micro-batch) and a 2-minute watermark on event time:
  per_minute  tumbling 1-minute windows: changes by type, bots, anonymous editors, reverts
  per_wiki    tumbling 5-minute windows: changes per wiki
  bursts      sliding 10-minute windows every 5 minutes: pages with at least 5 edits from at least 3 editors
Output mode is append, so a window is written once, after the watermark passes its end; anything arriving more than
2 minutes behind the latest event time is dropped. trigger(availableNow) processes every file present, then stops.
"""
import shutil
import time
from pathlib import Path

import pandas as pd
from pyspark.sql import SparkSession, functions as F, types as T

HERE = Path(__file__).parent
import os
LAND = Path(os.environ.get("LANDING", HERE / "data" / "landing"))                    # LANDING=... to test on a copy
OUT = Path(os.environ.get("OUTPUT", HERE / "data" / "out"))                        # OUTPUT=... for the late-data test
CK = OUT.parent / (OUT.name + "_checkpoints")
PROGRESS = os.environ.get("PROGRESS", "stream_progress.csv")
SCHEMA = T.StructType([T.StructField("id", T.LongType()), T.StructField("type", T.StringType()),
                       T.StructField("namespace", T.IntegerType()), T.StructField("title", T.StringType()),
                       T.StructField("wiki", T.StringType()), T.StructField("server_name", T.StringType()),
                       T.StructField("bot", T.BooleanType()), T.StructField("minor", T.BooleanType()),
                       T.StructField("patrolled", T.BooleanType()), T.StructField("timestamp", T.LongType()),
                       T.StructField("event_time", T.StringType()), T.StructField("anonymous", T.BooleanType()),
                       T.StructField("bytes_change", T.LongType()), T.StructField("revert", T.BooleanType()),
                       T.StructField("user_hash", T.StringType())])
WATERMARK = "2 minutes"


def spark():
    return (SparkSession.builder.master("local[4]").appName("wiki-stream")
            .config("spark.sql.session.timeZone", "UTC").config("spark.sql.shuffle.partitions", "8")
            .config("spark.ui.showConsoleProgress", "false").getOrCreate())


def main():
    for p in (OUT, CK):
        shutil.rmtree(p, ignore_errors=True)
    s = spark()
    events = (s.readStream.schema(SCHEMA).option("maxFilesPerTrigger", 3).json(str(LAND))
               .withColumn("event_time", F.to_timestamp("event_time"))
               .withWatermark("event_time", WATERMARK))
    edits = events.where(F.col("type").isin("edit", "new"))

    per_minute = (events.groupBy(F.window("event_time", "1 minute").alias("w"), "type")
                  .agg(F.count("*").alias("changes"), F.sum(F.col("bot").cast("int")).alias("bot"),
                       F.sum(F.col("anonymous").cast("int")).alias("anonymous"), F.sum(F.col("revert").cast("int")).alias("reverts"))
                  .select(F.col("w.start").alias("minute"), "type", "changes", "bot", "anonymous", "reverts"))
    per_wiki = (events.groupBy(F.window("event_time", "5 minutes").alias("w"), "wiki").agg(F.count("*").alias("changes"))
                .select(F.col("w.start").alias("window_start"), "wiki", "changes"))
    bursts = (edits.where(~F.col("bot"))
              .groupBy(F.window("event_time", "10 minutes", "5 minutes").alias("w"), "wiki", "namespace", "title")
              .agg(F.count("*").alias("edits"), F.approx_count_distinct("user_hash").alias("editors"),
                   F.sum(F.col("revert").cast("int")).alias("reverts"), F.sum(F.col("anonymous").cast("int")).alias("anonymous_edits"))
              .where((F.col("edits") >= 5) & (F.col("editors") >= 3))
              .select(F.col("w.start").alias("window_start"), "wiki", "namespace", "title", "edits", "editors", "reverts", "anonymous_edits"))

    queries = []
    for name, df in [("per_minute", per_minute), ("per_wiki", per_wiki), ("bursts", bursts)]:
        q = (df.writeStream.outputMode("append").format("parquet").option("path", str(OUT / name))
               .option("checkpointLocation", str(CK / name)).trigger(availableNow=True).queryName(name).start())
        queries.append(q)
    t0 = time.perf_counter()
    for q in queries:
        q.awaitTermination()
    secs = time.perf_counter() - t0
    rows = []
    for q in queries:
        for p in q.recentProgress:
            rows.append({"query": q.name, "batch": p["batchId"], "input_rows": p["numInputRows"],
                         "rows_per_second": round(p.get("processedRowsPerSecond") or 0, 1),
                         "watermark": p.get("eventTime", {}).get("watermark"),
                         "late_rows_dropped": sum(o.get("numRowsDroppedByWatermark", 0) for o in p.get("stateOperators", []))})
    prog = pd.DataFrame(rows)
    (HERE / "results").mkdir(exist_ok=True)
    prog.to_csv(HERE / "results" / PROGRESS, index=False)
    print(prog.groupby("query").agg(batches=("batch", "nunique"), rows=("input_rows", "sum"), dropped=("late_rows_dropped", "sum")))
    print(f"streamed in {secs:.0f} s")


if __name__ == "__main__":
    main()
