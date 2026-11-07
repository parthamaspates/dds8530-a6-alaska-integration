"""Generate notebooks/A6_Alaska_Integration.ipynb (executed later with nbconvert inside the app container)."""
import nbformat as nbf
from pathlib import Path

cells = []
def md(s): cells.append(nbf.v4.new_markdown_cell(s.strip()))
def code(s): cells.append(nbf.v4.new_code_cell(s.strip()))

md("""
# Assignment 6: Implementing Big Data Integration Techniques
**DDS-8530 Big Data Integration · Sina Zareian · National University**

Data: the **Alaska benchmark** (Crescenzi et al., 2021): 69,616 product specifications (camera, monitor, notebook) scraped from 68 e-commerce websites, with entity-resolution and schema-matching ground truth.

To reproduce a realistic integration problem, each vertical is stored in a different system and one website per vertical is replayed later as a stream. Spark runs on a standalone cluster (one master, two worker containers):

| Vertical | Stored in | Accessed with |
|---|---|---|
| camera | PostgreSQL (relational, EAV tables) | SQLAlchemy |
| monitor | MongoDB (one document per listing) | PyMongo |
| notebook | CSV partner feed (long format) | Pandas / Dask |
| 3 held-out websites | REST API that replays their listings → Kafka topic | kafka-python, Spark Structured Streaming |

Sections: 1 ingestion and engine comparison · 2 heterogeneous sources · 3 extraction and unification · 4 cleaning · 5 schema matching · 6 unified schema · 7 entity resolution (Pandas vs. Spark) · 8 real-time integration (Kafka + Spark) · 9 visual analysis · 10 summary.
""")
code("""
import os, sys, json, time, platform
sys.path.insert(0, os.path.abspath("../src"))
import numpy as np, pandas as pd
from IPython.display import Image, display
from alaska_int import config, loaders, stores, clean, schema_match, er, viz
from alaska_int.util import timer, TIMINGS, spark_session, cluster_info, save_json, log
RUNS = {}
def bench(name, fn, reps=3):
    # run fn reps times; keep every run and store the median in TIMINGS
    import statistics
    out = None
    for i in range(reps):
        t0 = time.perf_counter(); out = fn(); RUNS.setdefault(name, []).append(round(time.perf_counter() - t0, 3))
    TIMINGS[name] = statistics.median(RUNS[name])
    log.info("%s: runs %s, median %.2f s", name, RUNS[name], TIMINGS[name])
    return out
SKIP_STREAM = os.getenv("SKIP_STREAM") == "1"
pd.set_option("display.width", 160); pd.set_option("display.max_colwidth", 60)
print(platform.platform(), "| Python", platform.python_version(), "| CPUs:", os.cpu_count())
print("PostgreSQL:", config.PG_URL.split("@")[-1], "| MongoDB:", config.MONGO_URI, "| Kafka:", config.KAFKA_BOOTSTRAP)
""")

md("## 1. Ingestion: Pandas vs. Dask vs. Spark on the raw JSON data lake")
code("""
with timer("ingest_pandas"):
    raw = loaders.load_pandas()
with timer("ingest_dask"):
    raw_dask = loaders.load_dask(npartitions=16).compute()
spark = spark_session("alaska-a6", kafka=not SKIP_STREAM)
CLUSTER = cluster_info(spark)
print("Spark cluster:", CLUSTER)
with timer("ingest_spark"):
    rdd, raw_sdf = loaders.load_spark_rdd(spark)
    n_spark = raw_sdf.count()
print(f"rows: pandas={len(raw):,}  dask={len(raw_dask):,}  spark={n_spark:,}")
del raw_dask                       # keep one in-memory copy only
print(f"listings={raw.spec_id.nunique():,}  sources={raw.source.nunique()}  distinct attribute names={raw.attribute.nunique():,}")
raw.groupby("vertical").agg(listings=("spec_id", "nunique"), sources=("source", "nunique"), attribute_rows=("value", "size"))
""")
code("""
# RDD API: most frequent attribute names per vertical with map/reduceByKey
top_attrs = (rdd.map(lambda r: ((r[1], r[3]), 1)).reduceByKey(lambda a, b: a + b)
                .map(lambda kv: (kv[0][0], (kv[1], kv[0][1]))).groupByKey()
                .mapValues(lambda xs: [a for _, a in sorted(xs, reverse=True)[:8]]).collect())
for v, attrs in sorted(top_attrs):
    print(f"{v:9s}", attrs)
""")

md("## 2. Seeding the heterogeneous sources")
code("""
batch = raw[~raw.source.isin(config.STREAM_SOURCES.values())]
stream_raw = raw[raw.source.isin(config.STREAM_SOURCES.values())]
with timer("seed_postgres"):
    pg_specs, pg_rows = stores.seed_postgres(batch[batch.vertical == "camera"])
with timer("seed_mongo"):
    mongo_docs = stores.seed_mongo(batch[batch.vertical == "monitor"])
with timer("seed_csv"):
    stores.seed_csv(batch[batch.vertical == "notebook"])
n_stream = stores.seed_stream_holdout(stream_raw)
print(f"PostgreSQL camera listings: {pg_specs:,} ({pg_rows:,} attribute rows) | MongoDB monitor documents: {mongo_docs:,} | streaming hold-out: {n_stream:,}")
""")

md("## 3. Extraction with SQLAlchemy, PyMongo and Pandas/Dask, and unification into one format")
code("""
from sqlalchemy import text
with timer("extract_postgres"):
    pg = stores.read_postgres()
with stores.engine().connect() as con:
    display(pd.read_sql(text("SELECT source, COUNT(*) AS listings FROM specs GROUP BY source ORDER BY listings DESC LIMIT 5"), con))
with timer("extract_mongo"):
    mg = stores.read_mongo()
agg = list(stores.mongo_db()["specs"].aggregate([{"$group": {"_id": "$source", "n": {"$sum": 1}}}, {"$sort": {"n": -1}}, {"$limit": 5}]))
print("MongoDB aggregation (top sources):", [(a["_id"], a["n"]) for a in agg])
with timer("extract_csv_pandas"):
    cs = stores.read_csv_feed()
with timer("extract_csv_dask"):
    cs_dask = stores.read_csv_feed(use_dask=True)
assert len(cs) == len(cs_dask)
unified_long = pd.concat([pg.assign(origin="postgresql"), mg.assign(origin="mongodb"), cs.assign(origin="csv")], ignore_index=True)
del pg, mg, cs, cs_dask
import gc; gc.collect()
print("unified long table:", unified_long.shape, "| by origin:", unified_long.groupby("origin").spec_id.nunique().to_dict())
unified_long.sample(5, random_state=1)
""")

md("## 4. Cleaning: missing values, duplicates and inconsistencies")
code("""
with timer("clean_pandas"):
    cleaned, clean_report = clean.clean_long(unified_long)
unified_raw_rows = len(unified_long)
import dask.dataframe as dd
with timer("clean_dask_values"):
    ddf = dd.from_pandas(unified_long[["attribute", "value"]], npartitions=8)
    _ = ddf.map_partitions(lambda p: p.assign(attribute=clean.norm_attribute(p.attribute), value=clean.norm_value(p.value))).compute()
del ddf, _, unified_long; gc.collect()
pd.Series(clean_report)
""")
code("""
# inconsistent formats of the same property across sources
ex = cleaned[cleaned.attribute.isin(["screen size", "display size", "item weight", "weight"])].groupby("attribute").value.apply(lambda s: s.drop_duplicates().head(6).tolist())
ex
""")

md("## 5. Schema matching (unsupervised, name-based) evaluated against the Alaska ground truth")
code("""
sm_gt = loaders.load_ground_truth("schema_matching")
targets = sm_gt.groupby("vertical").target_attribute_name.apply(lambda s: sorted(set(s))).to_dict()
with timer("schema_matching"):
    sm_pred = schema_match.match(cleaned, targets)
sm_eval = schema_match.evaluate(sm_pred, sm_gt)
print(f"{len(sm_pred):,} source attributes, {sm_pred.target.notna().sum():,} mapped to a target")
display(sm_eval)
sm_pred[sm_pred.target.notna()].sample(8, random_state=3)[["source_attribute_id", "target", "score"]]
""")

md("## 6. Unified schema: brand, screen size (in), weight (kg)")
code("""
SCREEN = {"screen_size", "screen_size_diagonal", "display_size"}
WEIGHT = {"weight", "monitor_weight", "monitor_weight_with_stand"}
m = cleaned.merge(sm_pred[["source_attribute_id", "target"]], how="left",
                  left_on=cleaned.source + "//" + cleaned.attribute, right_on="source_attribute_id")
def first(mask):
    return m[mask].drop_duplicates("spec_id").set_index("spec_id").value
specs = cleaned.drop_duplicates("spec_id")[["spec_id", "vertical", "source", "origin"]].set_index("spec_id")
specs["title"] = first(m.attribute == "<page title>")
# naive integration: only attributes literally named like the target
naive = pd.DataFrame(index=specs.index)
naive["brand"] = first(m.attribute == "brand")
naive["screen_size_in"] = first(m.attribute == "screen size").map(clean.parse_inches)
naive["weight_kg"] = first(m.attribute == "weight").map(clean.parse_kg)
# integrated: schema-matched attributes + unit normalization + brand recovered from titles (filled in section 7)
# integrated: schema-matched attributes first, then exact names, then values recovered from the page title;
# all values pass through the same unit normalization (inches, kg) and brand canonicalization
specs["brand_attr"] = first(m.target == "brand").map(clean.canonical_brand)
specs["screen_size_in"] = (first(m.target.isin(SCREEN)).map(clean.parse_inches)
                           .combine_first(naive["screen_size_in"])
                           .combine_first(specs["title"].map(clean.parse_inches_in_title)))
specs["weight_kg"] = first(m.target.isin(WEIGHT)).map(clean.parse_kg).combine_first(naive["weight_kg"])
# plausibility ranges per vertical catch parsing errors such as eBay's '12 1"' (= 12.1 in)
RANGE = {"camera": (1, 5), "monitor": (10, 60), "notebook": (10, 20)}
for v, (lo, hi) in RANGE.items():
    for frame in (specs, naive):
        sel = frame.index.isin(specs.index[specs.vertical == v])
        frame.loc[sel, "screen_size_in"] = frame.loc[sel, "screen_size_in"].where(lambda x: x.between(lo, hi))
print("implausible screen sizes removed by range checks per vertical:", RANGE)
specs["n_attributes"] = cleaned.groupby("spec_id").size()
specs.head()
""")

md("## 7. Entity resolution: Pandas vs. Spark DataFrames")
code("""
er_gt = loaders.load_ground_truth("entity_resolution")
resolved, vocab, dfreq = bench("er_pandas", lambda: er.resolve_pandas(cleaned))
er_eval = er.evaluate(resolved, er_gt)
display(er_eval)
clean_sdf = spark.createDataFrame(cleaned[["spec_id", "vertical", "source", "attribute", "value"]])
clean_sdf = clean_sdf.repartition(8).cache(); clean_sdf.count()
resolved_s = bench("er_spark", lambda: er.resolve_spark(spark, clean_sdf, vocab).toPandas())
er_eval_spark = er.evaluate(resolved_s, er_gt)
agree = resolved.merge(resolved_s[["spec_id", "entity_id"]], on="spec_id", suffixes=("", "_spark"))
print(f"Spark vs Pandas entity assignment agreement: {(agree.entity_id == agree.entity_id_spark).mean():.4f}")
display(er_eval_spark)
sizes = resolved[resolved.block_key.notna()].groupby("block_key").size()
print(f"entities with >=2 listings: {(sizes>=2).sum():,}; listings in them: {sizes[sizes>=2].sum():,} of {len(resolved):,}; largest: {sizes.max()}")
""")
code("""
# join + aggregate benchmark: attribute rows x schema-matching ground truth, counted per target attribute
gt_ids = sm_gt.rename(columns={"source_attribute_id": "sid"})[["sid", "target_attribute_name"]]
long_ids = cleaned.assign(sid=cleaned.source + "//" + cleaned.attribute)[["spec_id", "sid"]]
j1 = bench("join_pandas", lambda: long_ids.merge(gt_ids, on="sid").groupby("target_attribute_name").spec_id.nunique())
j2 = bench("join_dask", lambda: dd.from_pandas(long_ids, npartitions=8).merge(gt_ids, on="sid").groupby("target_attribute_name").spec_id.nunique().compute())
from pyspark.sql import functions as F
j3 = bench("join_spark", lambda: (spark.createDataFrame(long_ids).join(F.broadcast(spark.createDataFrame(gt_ids)), "sid")
              .groupBy("target_attribute_name").agg(F.countDistinct("spec_id").alias("n")).toPandas()))
print("same result:", j1.sort_index().equals(j2.sort_index()), len(j1) == len(j3))
clean_sdf.unpersist(); del clean_sdf                 # free executor memory before the streaming section
""")
code("""
specs = specs.join(resolved.set_index("spec_id")[["brand", "block_key", "entity_id"]])
specs["brand"] = specs["brand_attr"].where(specs["brand_attr"].notna(), specs["brand"])
fields = ["brand", "screen_size_in", "weight_kg"]
applies = {"brand": ["camera", "monitor", "notebook"], "screen_size_in": ["camera", "monitor", "notebook"], "weight_kg": ["monitor", "notebook"]}
rows = []
for f in fields:
    idx = specs.vertical.isin(applies[f])
    rows.append({"field": f, "stage": "naive (exact names)", "pct_missing": 100 * naive.loc[idx, f].isna().mean()})
    rows.append({"field": f, "stage": "integrated", "pct_missing": 100 * specs.loc[idx, f].isna().mean()})
quality = pd.DataFrame(rows)
specs.reset_index().to_parquet(config.OUT_DIR / "unified_specs.parquet")
quality.pivot(index="field", columns="stage", values="pct_missing").round(1)
""")

md("## 8. Streaming integration: replayed listing API → Kafka → Spark Structured Streaming → Parquet + MongoDB")
code("""
from alaska_int import producer, stream_job
catalog = resolved[resolved.block_key.notna()].groupby("block_key").agg(entity_id=("entity_id", "first"), entity_size=("spec_id", "size")).reset_index()
if SKIP_STREAM:
    print("SKIP_STREAM=1: streaming section skipped (no Kafka broker in this environment)")
    stream_metrics, windows, prod, elapsed = pd.DataFrame(columns=["t","rows","matched"]), pd.DataFrame(), {}, 0
else:
    stream_metrics, windows, prod, elapsed = stream_job.run(spark, catalog, vocab, dfreq, expected=n_stream, producer_fn=producer.run)
    print(f"producer: {prod} | processed {stream_metrics.rows.sum():,} listings in {len(stream_metrics)} micro-batches, {elapsed:.0f} s")
    print(f"matched to an existing entity: {stream_metrics.matched.sum()/stream_metrics.rows.sum():.1%} | throughput: {stream_metrics.rows.sum()/elapsed:.1f} listings/s | max end-to-end lag: {stream_metrics.max_lag_s.max():.1f} s")
    print("MongoDB stream_integrated documents:", stores.mongo_db()["stream_integrated"].count_documents({}))
    display(stream_metrics.head(8))
""")
code("""
# accuracy of the real-time matches on the labeled streamed listings
if not SKIP_STREAM:
    st = pd.read_parquet(config.OUT_DIR / "stream" / "integrated")
    lab = er_gt.drop_duplicates("spec_id").set_index("spec_id").entity_id   # the ground truth lists some listings twice
    cat_lab = resolved.assign(true=resolved.spec_id.map(lab)).dropna(subset=["true"])
    majority = cat_lab.groupby("entity_id").true.agg(lambda s: s.mode().iat[0])
    s_lab = st.assign(true=st.spec_id.map(lab)).dropna(subset=["true"])
    m_lab = s_lab[s_lab.matched & s_lab.entity_id.isin(majority.index)]
    stream_acc = {"labeled_streamed": len(s_lab), "matched_with_labeled_entity": len(m_lab),
                  "correct": int((m_lab.entity_id.map(majority) == m_lab.true).sum())}
    stream_acc["precision"] = round(stream_acc["correct"] / max(1, len(m_lab)), 3)
    print(stream_acc)
else:
    stream_acc = {}
""")

md("## 9. Visual analysis")
code("""
spec_counts = specs.reset_index().groupby(["vertical", "source", "origin"]).size().rename("n").reset_index()
streamed = stream_raw.groupby(["vertical", "source"]).spec_id.nunique().rename("n").reset_index().assign(origin="kafka stream")
figs = [viz.sources_overview(pd.concat([spec_counts, streamed]), specs.reset_index()[["vertical", "n_attributes"]]),
        viz.quality(quality), viz.accuracy(sm_eval, er_eval)]
perf = pd.DataFrame([
    ("Ingest 69.6k JSON files", "Pandas", TIMINGS["ingest_pandas"]), ("Ingest 69.6k JSON files", "Dask", TIMINGS["ingest_dask"]), ("Ingest 69.6k JSON files", "Spark", TIMINGS["ingest_spark"]),
    ("Clean values", "Pandas", TIMINGS["clean_pandas"]), ("Clean values", "Dask", TIMINGS["clean_dask_values"]),
    ("Join + aggregate", "Pandas", TIMINGS["join_pandas"]), ("Join + aggregate", "Dask", TIMINGS["join_dask"]), ("Join + aggregate", "Spark", TIMINGS["join_spark"]),
    ("Entity resolution", "Pandas", TIMINGS["er_pandas"]), ("Entity resolution", "Spark", TIMINGS["er_spark"])],
    columns=["task", "engine", "seconds"])
figs.append(viz.performance(perf))
if not SKIP_STREAM:
    figs.append(viz.streaming(stream_metrics, windows))
figs.append(viz.screen_sizes(specs.reset_index()))
for f in figs:
    display(Image(filename=str(f), width=900))
""")

md("## 10. Summary")
code("""
summary = {
    "listings": int(raw.spec_id.nunique()), "sources": int(raw.source.nunique()), "attribute_rows": int(len(raw)),
    "clean_report": clean_report, "schema_matching": sm_eval.to_dict("records"), "entity_resolution": er_eval.to_dict("records"),
    "entity_resolution_spark": er_eval_spark.to_dict("records"), "quality": quality.to_dict("records"),
    "timings_s": TIMINGS, "timing_runs_s": RUNS, "spark_cluster": CLUSTER, "stream": {"listings": int(stream_metrics.rows.sum()) if len(stream_metrics) else 0,
        "batches": int(len(stream_metrics)), "elapsed_s": round(elapsed, 1), "producer": prod,
        "match_rate": round(float(stream_metrics.matched.sum() / max(1, stream_metrics.rows.sum())), 3) if len(stream_metrics) else None,
        "max_lag_s": float(stream_metrics.max_lag_s.max()) if len(stream_metrics) else None, "accuracy": stream_acc},
}
save_json(summary, "metrics.json")
spark.stop()
pd.DataFrame(perf).pivot(index="task", columns="engine", values="seconds")
""")

nb = nbf.v4.new_notebook(cells=cells, metadata={"kernelspec": {"name": "python3", "display_name": "Python 3", "language": "python"}})
out = Path(__file__).resolve().parents[1] / "notebooks" / "A6_Alaska_Integration.ipynb"
nbf.write(nb, out)
print("wrote", out)
