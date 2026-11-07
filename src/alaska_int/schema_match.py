"""Schema matching: map every source attribute (source//attribute) to a target attribute of the mediated schema.

Unsupervised and name-based: attribute names are normalized, expanded with a small synonym list and compared with
the target names using a token-set similarity (RapidFuzz). The ground truth is used only for evaluation.
"""
import re
import pandas as pd
from rapidfuzz import fuzz, process

SYNONYMS = {
    "manufacturer": "brand", "brand name": "brand", "make": "brand", "maker": "brand", "mfr": "brand",
    "display size": "screen size", "screen": "screen size", "lcd screen size": "screen size",
    "viewable size": "screen size diagonal", "diagonal size": "screen size diagonal",
    "item weight": "weight", "weight approx": "weight", "product weight": "weight",
    "megapixels": "image resolution", "effective pixels": "image resolution", "resolution": "image resolution",
    "processor": "cpu type", "processor type": "cpu type", "processor speed": "cpu frequency",
    "memory": "ram capacity", "ram": "ram capacity", "installed memory": "ram capacity",
    "hard drive": "hdd capacity", "hard drive capacity": "hdd capacity", "storage": "hdd capacity",
    "optical zoom": "zoom optical", "digital zoom": "zoom digital",
}

def _prep(name):
    n = re.sub(r"[^a-z0-9 ]", " ", name.lower())
    n = re.sub(r"\s+", " ", n).strip()
    return SYNONYMS.get(n, n)

def match(long_df, targets_by_vertical, threshold=70):
    """Return one row per source attribute with its predicted target (or None) and the similarity score."""
    sa = long_df[long_df.attribute != "<page title>"][["vertical", "source", "attribute"]].drop_duplicates()
    out = []
    for v, g in sa.groupby("vertical"):
        targets = targets_by_vertical[v]
        choices = {t: t.replace("_", " ") for t in targets}
        cache = {}
        for src, attr in zip(g.source, g.attribute):
            key = _prep(attr)
            if key not in cache:
                best = process.extractOne(key, choices, scorer=fuzz.token_set_ratio)
                cache[key] = (best[2], best[1]) if best else (None, 0)
            t, score = cache[key]
            out.append((v, f"{src}//{attr}", attr, t if score >= threshold else None, score))
    return pd.DataFrame(out, columns=["vertical", "source_attribute_id", "attribute", "target", "score"])

def evaluate(pred, gt):
    """Precision/recall/F1 on the source attributes that appear in the ground truth."""
    rows = []
    for v, g in gt.groupby("vertical"):
        truth = set(zip(g.source_attribute_id, g.target_attribute_name))
        p = pred[(pred.vertical == v) & pred.source_attribute_id.isin(set(g.source_attribute_id)) & pred.target.notna()]
        guess = set(zip(p.source_attribute_id, p.target))
        tp = len(guess & truth)
        P = tp / len(guess) if guess else 0.0
        R = tp / len(truth) if truth else 0.0
        rows.append({"vertical": v, "gt_pairs": len(truth), "predicted": len(guess), "correct": tp,
                     "precision": round(P, 3), "recall": round(R, 3),
                     "f1": round(2 * P * R / (P + R), 3) if P + R else 0.0})
    return pd.DataFrame(rows)
