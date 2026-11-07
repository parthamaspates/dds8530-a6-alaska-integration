"""Entity resolution: decide which listings describe the same real-world product.

1. brand  : canonical brand from a brand-like attribute, else the first known brand word in the page title
2. codes  : model-code candidates (tokens and adjacent-token pairs mixing letters and digits, minus units/years)
3. blocking key : (brand, code) where the chosen code maximizes log(df) + 0.35*len(code); df = number of listings
                  of that brand sharing the code. Common enough to be the product's model number, long enough
                  to be specific.
4. listings with the same key form one entity (no transitive closure, which chains unrelated products).
"""
import itertools, math, re
from collections import Counter, defaultdict
import pandas as pd
from .clean import canonical_brand

BRAND_ATTRS = ["brand", "manufacturer", "brand name", "make", "maker", "mfr"]
CODE_ATTRS = ["<page title>", "model", "mpn", "model number", "item model number", "product model",
              "part number", "manufacturer part number"]
TOK = re.compile(r"[A-Za-z0-9]+")
UNIT = re.compile(r"^(\d+(\.\d+)?(GB|TB|MB|MP|MM|HZ|P|K|W|IN|INCH|INCHES|CM|G|LBS|LB|KG|GHZ|MHZ|X|FPS|V|MAH|NITS|MS"
                  r"|BIT|RPM|PCS|MPIX|OZ)?|\d+X\d+(X\d+)?|(19|20)\d\d|USB\d+|DDR\d\w*|HDMI\d?|I[3579]|PS\d|WIN\d+"
                  r"|WINDOWS\d+|\d+(ST|ND|RD|TH)|X\d|F\d(\.\d)?|IP\d\d)$")

def spec_table(long_df):
    """One row per listing with the fields ER needs."""
    keep = long_df[long_df.attribute.isin(set(BRAND_ATTRS) | set(CODE_ATTRS))]
    wide = keep.pivot_table(index="spec_id", columns="attribute", values="value", aggfunc="first")
    meta = long_df[["spec_id", "vertical", "source"]].drop_duplicates("spec_id").set_index("spec_id")
    return meta.join(wide, how="left").reset_index()

def brand_vocabulary(specs, min_count=5):
    c = Counter()
    for a in BRAND_ATTRS:
        if a in specs:
            c.update(specs[a].dropna().map(canonical_brand).dropna())
    return {b for b, n in c.items() if n >= min_count and len(b) >= 2 and not b.isdigit()}

def brand_of(row, vocab):
    for a in BRAND_ATTRS:
        b = canonical_brand(row.get(a))
        if isinstance(b, str) and b in vocab:
            return b
    for w in TOK.findall(str(row.get("<page title>", "")).lower())[:8]:
        b = canonical_brand(w)
        if b in vocab:
            return b
    return None

def model_codes(row):
    text = " ".join(str(row.get(a, "")) for a in CODE_ATTRS if isinstance(row.get(a), str))
    toks = [t.upper() for t in TOK.findall(text)]
    out = set()
    for c in toks + [a + b for a, b in zip(toks, toks[1:])]:
        if 4 <= len(c) <= 15 and re.search(r"[A-Z]", c) and re.search(r"\d", c) and not UNIT.match(c):
            out.add(c)
    return sorted(out)

def code_score(df, code):
    return math.log(df) + 0.35 * len(code)

def resolve_pandas(long_df):
    """Return (per-listing table with brand, codes, key, entity_id, vocabulary, code document frequencies)."""
    specs = spec_table(long_df)
    vocab = brand_vocabulary(specs)
    recs = specs.to_dict("records")
    specs["brand"] = [brand_of(r, vocab) for r in recs]
    specs["codes"] = [model_codes(r) for r in recs]
    dfreq = Counter((v, b, c) for v, b, cs in zip(specs.vertical, specs.brand, specs.codes) for c in cs)
    keys = []
    for v, b, cs in zip(specs.vertical, specs.brand, specs.codes):
        cand = [c for c in cs if dfreq[(v, b, c)] >= 2]
        if not cand:
            keys.append(None)
            continue
        c = max(cand, key=lambda c: (code_score(dfreq[(v, b, c)], c), c))
        keys.append(f"{v}|{b or '?'}|{c}")
    specs["block_key"] = keys
    specs["entity_id"] = specs["block_key"].fillna("singleton|" + specs["spec_id"])
    return specs[["spec_id", "vertical", "source", "brand", "codes", "block_key", "entity_id"]], vocab, dfreq

def resolve_spark(spark, long_sdf, vocab):
    """Same algorithm with Spark DataFrames: UDF for features, groupBy for document frequency, window to pick the key."""
    from pyspark.sql import functions as F, types as T, Window
    vocab_b = spark.sparkContext.broadcast(vocab)
    attrs = list(set(BRAND_ATTRS) | set(CODE_ATTRS))
    w = (long_sdf.filter(F.col("attribute").isin(attrs))
         .groupBy("spec_id", "vertical").agg(F.map_from_entries(F.collect_list(F.struct("attribute", "value"))).alias("m")))

    @F.udf(T.StructType([T.StructField("brand", T.StringType()), T.StructField("codes", T.ArrayType(T.StringType()))]))
    def feats(m):
        row = dict(m or {})
        return (brand_of(row, vocab_b.value), model_codes(row))

    f = w.withColumn("f", feats("m")).select("spec_id", "vertical", F.col("f.brand").alias("brand"), F.col("f.codes").alias("codes"))
    ex = f.select("spec_id", "vertical", F.coalesce("brand", F.lit("?")).alias("b"), F.explode("codes").alias("code"))
    dfq = ex.groupBy("vertical", "b", "code").agg(F.count("*").alias("df")).filter("df >= 2")
    scored = (ex.join(dfq, ["vertical", "b", "code"])
                .withColumn("score", F.log("df") + F.lit(0.35) * F.length("code")))
    win = Window.partitionBy("spec_id").orderBy(F.desc("score"), F.desc("code"))
    best = (scored.withColumn("r", F.row_number().over(win)).filter("r = 1")
                  .select("spec_id", F.concat_ws("|", "vertical", "b", "code").alias("block_key")))
    return (f.join(best, "spec_id", "left")
             .withColumn("entity_id", F.coalesce("block_key", F.concat(F.lit("singleton|"), "spec_id"))))

def evaluate(resolved, gt):
    """Pairwise precision/recall/F1 over the listings labeled in the ground truth."""
    rows = []
    ent = resolved.set_index("spec_id")["entity_id"]
    for v, g in gt.groupby("vertical"):
        g = g[g.spec_id.isin(ent.index)]
        true = set()
        for _, s in g.groupby("entity_id"):
            true |= set(itertools.combinations(sorted(s.spec_id), 2))
        pred = set()
        lab = pd.DataFrame({"spec_id": g.spec_id.unique()})
        lab["e"] = lab.spec_id.map(ent)
        for _, s in lab.groupby("e"):
            pred |= set(itertools.combinations(sorted(s.spec_id), 2))
        tp = len(true & pred)
        P = tp / len(pred) if pred else 0.0
        R = tp / len(true) if true else 0.0
        rows.append({"vertical": v, "labeled_listings": len(lab), "true_pairs": len(true), "predicted_pairs": len(pred),
                     "precision": round(P, 3), "recall": round(R, 3), "f1": round(2 * P * R / (P + R), 3) if P + R else 0.0})
    return pd.DataFrame(rows)
