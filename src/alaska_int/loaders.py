"""Three ways to ingest the ~69,600 JSON specification files: plain Python + Pandas, Dask, and Spark RDDs."""
import json, os
from pathlib import Path
import pandas as pd
from . import config

COLS = ["spec_id", "vertical", "source", "attribute", "value"]

def spec_files(verticals=config.VERTICALS, include_stream=True):
    """Yield (vertical, source, path) for every specification file."""
    for v in verticals:
        base = config.RAW_DIR / v / f"{v}_specs"
        for source in sorted(os.listdir(base)):
            if not include_stream and config.STREAM_SOURCES.get(v) == source:
                continue
            for f in sorted(os.listdir(base / source)):
                yield v, source, base / source / f

def parse_file(item):
    """One JSON file -> list of long-format rows (spec_id, vertical, source, attribute, value)."""
    v, source, path = item
    spec_id = f"{v}:{source}//{Path(path).stem}"     # vertical prefix: file ids repeat across verticals
    with open(path, encoding="utf-8") as fh:
        doc = json.load(fh)
    return [(spec_id, v, source, k, v2 if isinstance(v2, str) else json.dumps(v2)) for k, v2 in doc.items()]

def load_pandas(verticals=config.VERTICALS, include_stream=True):
    rows = []
    for item in spec_files(verticals, include_stream):
        rows.extend(parse_file(item))
    return pd.DataFrame(rows, columns=COLS)

def load_dask(verticals=config.VERTICALS, include_stream=True, npartitions=16):
    import dask.bag as db
    items = list(spec_files(verticals, include_stream))
    bag = db.from_sequence(items, npartitions=npartitions).map(parse_file).flatten()
    meta = pd.DataFrame({c: pd.Series(dtype="object") for c in COLS})
    return bag.to_dataframe(meta=meta)

def load_spark_rdd(spark, verticals=config.VERTICALS):
    """wholeTextFiles RDD -> flatMap into long rows -> Spark DataFrame."""
    sc = spark.sparkContext
    rdds = []
    for v in verticals:
        pattern = str(config.RAW_DIR / v / f"{v}_specs" / "*" / "*.json")
        def to_rows(kv, v=v):
            path, text = kv
            parts = path.rstrip("/").split("/")
            source, sid = parts[-2], parts[-1][:-5]
            doc = json.loads(text)
            return [(f"{v}:{source}//{sid}", v, source, k, x if isinstance(x, str) else json.dumps(x)) for k, x in doc.items()]
        rdds.append(sc.wholeTextFiles(pattern, minPartitions=16).flatMap(to_rows))
    rdd = sc.union(rdds)
    return rdd, spark.createDataFrame(rdd, schema=COLS)

def load_ground_truth(kind, verticals=config.VERTICALS):
    """kind = 'entity_resolution' or 'schema_matching'."""
    frames = []
    for v in verticals:
        df = pd.read_csv(config.RAW_DIR / v / f"{v}_ground_truths" / f"{v}_{kind}_gt.csv")
        df.insert(0, "vertical", v)
        if kind == "entity_resolution":
            df["spec_id"] = v + ":" + df["spec_id"]
        frames.append(df)
    return pd.concat(frames, ignore_index=True)
