"""Seed the heterogeneous sources (PostgreSQL, MongoDB, CSV, streaming hold-out) and read them back."""
import json
import pandas as pd
from sqlalchemy import create_engine, text
from . import config
from .util import log

def mongo_client():
    if config.MONGO_URI.startswith("mongomock"):
        import mongomock
        return mongomock.MongoClient()
    from pymongo import MongoClient
    return MongoClient(config.MONGO_URI, serverSelectionTimeoutMS=10000)

_MONGO = None
def mongo_db():
    global _MONGO
    if _MONGO is None:
        _MONGO = mongo_client()
    return _MONGO[config.MONGO_DB]

def engine():
    return create_engine(config.PG_URL, future=True)

# ---------- seeding ----------
def seed_postgres(long_df):
    """Relational catalog: specs(spec_id, vertical, source, title) + spec_attributes (EAV)."""
    eng = engine()
    titles = long_df[long_df.attribute == "<page title>"][["spec_id", "value"]].rename(columns={"value": "title"})
    specs = long_df[["spec_id", "vertical", "source"]].drop_duplicates().merge(titles, on="spec_id", how="left")
    attrs = long_df[long_df.attribute != "<page title>"][["spec_id", "attribute", "value"]]
    with eng.begin() as con:
        con.execute(text("DROP TABLE IF EXISTS spec_attributes"))
        con.execute(text("DROP TABLE IF EXISTS specs"))
        con.execute(text("CREATE TABLE specs (spec_id VARCHAR(80) PRIMARY KEY, vertical VARCHAR(20), source VARCHAR(80), title TEXT)"))
        con.execute(text("CREATE TABLE spec_attributes (spec_id VARCHAR(80) REFERENCES specs(spec_id), attribute TEXT, value TEXT)"))
    specs.to_sql("specs", eng, if_exists="append", index=False, chunksize=5000, method="multi")
    attrs.to_sql("spec_attributes", eng, if_exists="append", index=False, chunksize=5000, method="multi")
    with eng.begin() as con:
        con.execute(text("CREATE INDEX IF NOT EXISTS ix_attr_spec ON spec_attributes(spec_id)"))
    log.info("PostgreSQL: %d specs, %d attribute rows", len(specs), len(attrs))
    return len(specs), len(attrs)

def seed_mongo(long_df):
    """Document store: one document per specification, attributes kept as a nested object."""
    col = mongo_db()["specs"]
    col.drop()
    docs = []
    for sid, g in long_df.groupby("spec_id", sort=False):
        docs.append({"_id": sid, "vertical": g.vertical.iat[0], "source": g.source.iat[0],
                     "spec": dict(zip(g.attribute, g.value))})
    for i in range(0, len(docs), 5000):
        col.insert_many(docs[i:i + 5000])
    col.create_index("source")
    log.info("MongoDB: %d documents", len(docs))
    return len(docs)

def seed_csv(long_df):
    path = config.SOURCES_DIR / "notebook_partner_feed.csv"
    long_df[["spec_id", "source", "attribute", "value"]].to_csv(path, index=False)
    log.info("CSV feed: %d rows -> %s", len(long_df), path.name)
    return path

def seed_stream_holdout(long_df):
    """Held-out sources become the live listing feed served by the feed API and pushed through Kafka."""
    path = config.SOURCES_DIR / "stream_listings.jsonl"
    with open(path, "w") as fh:
        for sid, g in long_df.groupby("spec_id", sort=False):
            fh.write(json.dumps({"spec_id": sid, "vertical": g.vertical.iat[0], "source": g.source.iat[0],
                                 "spec": dict(zip(g.attribute, g.value))}) + "\n")
    n = long_df.spec_id.nunique()
    log.info("Streaming hold-out: %d listings -> %s", n, path.name)
    return n

# ---------- extraction ----------
def read_postgres():
    q = text("""SELECT s.spec_id, s.vertical, s.source, a.attribute, a.value
                FROM specs s JOIN spec_attributes a ON a.spec_id = s.spec_id
                UNION ALL
                SELECT spec_id, vertical, source, '<page title>' AS attribute, title AS value FROM specs""")
    with engine().connect() as con:
        return pd.read_sql(q, con)

def read_mongo():
    rows = []
    for d in mongo_db()["specs"].find({}, {"vertical": 1, "source": 1, "spec": 1}):
        for k, v in d["spec"].items():
            rows.append((d["_id"], d["vertical"], d["source"], k, v))
    return pd.DataFrame(rows, columns=["spec_id", "vertical", "source", "attribute", "value"])

def read_csv_feed(use_dask=False):
    path = config.SOURCES_DIR / "notebook_partner_feed.csv"
    if use_dask:
        import dask.dataframe as dd
        df = dd.read_csv(path, dtype=str, blocksize="16MB", keep_default_na=False).compute()
    else:
        df = pd.read_csv(path, dtype=str, keep_default_na=False)
    df.insert(1, "vertical", "notebook")
    return df
