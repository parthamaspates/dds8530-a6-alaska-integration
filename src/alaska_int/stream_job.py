"""Spark Structured Streaming: consume live listings from Kafka, resolve each one against the batch entity catalog
in real time, write the integrated records to Parquet and MongoDB, and keep per-batch and windowed metrics."""
import math, shutil, threading, time
import pandas as pd
from . import config, er
from .stores import mongo_db
from .util import log

def run(spark, catalog, vocab, dfreq, expected, producer_fn, timeout_s=900):
    from pyspark.sql import functions as F, types as T
    cat_sdf = spark.createDataFrame(catalog[["block_key", "entity_id", "entity_size"]])
    vocab_b = spark.sparkContext.broadcast(vocab)
    dfreq_b = spark.sparkContext.broadcast({f"{v}|{b or '?'}|{c}": n for (v, b, c), n in dfreq.items() if n >= 2})

    schema = T.StructType([T.StructField("spec_id", T.StringType()), T.StructField("vertical", T.StringType()),
                           T.StructField("source", T.StringType()), T.StructField("event_time", T.StringType()),
                           T.StructField("spec", T.MapType(T.StringType(), T.StringType()))])

    @F.udf(T.StructType([T.StructField("brand", T.StringType()), T.StructField("block_key", T.StringType())]))
    def resolve(vertical, spec):
        row = dict(spec or {})
        b = er.brand_of(row, vocab_b.value)
        best, best_s = None, -1.0
        for c in er.model_codes(row):
            k = f"{vertical}|{b or '?'}|{c}"
            n = dfreq_b.value.get(k)
            if n and er.code_score(n, c) > best_s:
                best, best_s = k, er.code_score(n, c)
        return (b, best)

    from .producer import ensure_topic
    ensure_topic()                            # the topic must exist before the query subscribes
    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP)
           .option("subscribe", config.KAFKA_TOPIC)
           .option("startingOffsets", "latest").load())
    parsed = (raw.select(F.from_json(F.col("value").cast("string"), schema).alias("j"), F.col("timestamp").alias("kafka_ts"))
                 .select("j.*", "kafka_ts")
                 .withColumn("event_ts", F.to_timestamp("event_time"))
                 .withColumn("r", resolve("vertical", "spec"))
                 .withColumn("brand", F.col("r.brand")).withColumn("block_key", F.col("r.block_key")).drop("r")
                 .withColumn("title", F.col("spec").getItem("<page title>")))
    integrated = parsed.join(cat_sdf, "block_key", "left").withColumn("matched", F.col("entity_id").isNotNull())

    shutil.rmtree(config.OUT_DIR / "stream", ignore_errors=True)   # fresh checkpoint and sink on every run
    metrics, lock = [], threading.Lock()
    out_dir = str(config.OUT_DIR / "stream" / "integrated")
    coll = mongo_db()["stream_integrated"]
    coll.drop()

    def sink(batch_df, batch_id):
        t0 = time.time()
        pdf = batch_df.select("spec_id", "vertical", "source", "event_ts", "kafka_ts", "title", "brand",
                              "block_key", "entity_id", "entity_size", "matched").toPandas()
        if pdf.empty:
            return
        batch_df.drop("spec").write.mode("append").parquet(out_dir)
        coll.insert_many([{k: (None if (isinstance(v, float) and math.isnan(v)) else v) for k, v in r.items()}
                          for r in pdf.astype({"event_ts": str, "kafka_ts": str}).to_dict("records")])
        lag = (pd.Timestamp.now(tz="UTC") - pd.to_datetime(pdf.kafka_ts, utc=True)).dt.total_seconds()
        with lock:
            metrics.append({"batch_id": batch_id, "wall_time": time.time(), "rows": len(pdf),
                            "matched": int(pdf.matched.sum()), "max_lag_s": round(lag.max(), 2),
                            "write_s": round(time.time() - t0, 2),
                            **{f"n_{v}": int((pdf.vertical == v).sum()) for v in config.VERTICALS}})

    q1 = (integrated.writeStream.foreachBatch(sink).trigger(processingTime="5 seconds")
          .option("checkpointLocation", str(config.OUT_DIR / "stream" / "_chk_integrated")).start())
    q2 = (parsed.groupBy(F.window("event_ts", "10 seconds"), "vertical").count()
          .writeStream.outputMode("complete").format("memory").queryName("window_counts")
          .trigger(processingTime="5 seconds").start())

    while q1.lastProgress is None:          # wait until the query is reading from Kafka
        time.sleep(1)
    log.info("streaming queries started; starting the producer")
    prod_result = {}
    t_prod = threading.Thread(target=lambda: prod_result.update(producer_fn()))
    t_start = time.time()
    t_prod.start()
    while time.time() - t_start < timeout_s:
        with lock:
            done = sum(m["rows"] for m in metrics)
        if done >= expected:
            break
        time.sleep(2)
    t_prod.join(timeout=60)
    time.sleep(6)                            # let the windowed query see the last batch
    windows = spark.sql("SELECT window.start AS window_start, vertical, `count` FROM window_counts ORDER BY window_start").toPandas()
    q1.stop(); q2.stop()
    m = pd.DataFrame(metrics)
    m["t"] = m.wall_time - t_start
    log.info("stream: %d listings processed in %d micro-batches", m.rows.sum(), len(m))
    return m, windows, prod_result, time.time() - t_start
