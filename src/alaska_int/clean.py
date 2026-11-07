"""Cleaning and normalization: missing values, duplicates and inconsistent units/formats."""
import html, re
import numpy as np
import pandas as pd

PLACEHOLDERS = {"", "-", "--", "n/a", "na", "none", "null", "unknown", "not specified", "not available",
                "see description", "does not apply", "not applicable", "?", "."}
MOJIBAKE = {"Â±": "±", "Â°": "°", "â€\x9d": '"', "â€œ": '"', "â€™": "'", "Â": ""}

def norm_attribute(s: pd.Series) -> pd.Series:
    return (s.str.lower().str.replace(r"[:\s]+$", "", regex=True)
             .str.replace(r"\s+", " ", regex=True).str.strip())

def norm_value(s: pd.Series) -> pd.Series:
    out = s.astype(str).map(html.unescape)
    for bad, good in MOJIBAKE.items():
        out = out.str.replace(bad, good, regex=False)
    return out.str.replace(r"\s+", " ", regex=True).str.strip()

def clean_long(df: pd.DataFrame):
    """Return (clean long-format frame, report of what was fixed)."""
    rep = {"rows_in": len(df), "specs_in": df.spec_id.nunique()}
    df = df.copy()
    df["attribute"] = norm_attribute(df["attribute"])
    df["value"] = norm_value(df["value"])
    missing = df["value"].str.lower().isin(PLACEHOLDERS)
    rep["placeholder_values_removed"] = int(missing.sum())
    df = df[~missing]
    dup = df.duplicated(["spec_id", "attribute"])            # same attribute twice after normalization
    rep["duplicate_attribute_rows"] = int(dup.sum())
    df = df[~dup]
    # duplicate listings: identical attribute/value content inside the same source
    kv = (df["attribute"] + "=" + df["value"]).groupby(df["spec_id"]).agg(lambda x: "|".join(sorted(x)))
    sig = kv.map(hash).rename("sig").reset_index()
    sig = sig.merge(df[["spec_id", "source"]].drop_duplicates(), on="spec_id")
    dup_specs = sig[sig.duplicated(["source", "sig"])].spec_id
    rep["duplicate_listings_removed"] = int(len(dup_specs))
    df = df[~df.spec_id.isin(set(dup_specs))]
    rep["rows_out"] = len(df)
    rep["specs_out"] = df.spec_id.nunique()
    return df.reset_index(drop=True), rep

# ---------- unit parsers for the unified schema ----------
NUM = r"(\d+(?:[.,]\d+)?)"

def parse_inches(text):
    """'21.5''', '15.6 inches', '39.6 cm (15.6")', '27IN' -> inches (float) or NaN."""
    if not isinstance(text, str):
        return np.nan
    t = text.lower().replace(",", ".")
    m = re.search(NUM + r'\s*(?:"|\'\'|”|in\b|inch|inches|-inch)', t)
    if m:
        v = float(m.group(1))
    else:
        m = re.search(NUM + r"\s*cm", t)
        if m:
            v = float(m.group(1)) / 2.54
        else:
            m = re.fullmatch(r"\s*" + NUM + r"\s*", t)
            if not m:
                return np.nan
            v = float(m.group(1))
    return round(v, 1) if 1 <= v <= 100 else np.nan

def parse_kg(text):
    """'8 pounds', '2.1 kg', '500g (1.1 lbs)', '4.4 lbs' -> kilograms or NaN."""
    if not isinstance(text, str):
        return np.nan
    t = text.lower().replace(",", ".")
    hits = []
    for pat, f in [(NUM + r"\s*kg", 1.0), (NUM + r"\s*(?:lbs?|pounds?)\b", 0.45359237),
                   (NUM + r"\s*(?:g|grams?)\b", 0.001), (NUM + r"\s*oz", 0.0283495)]:
        m = re.search(pat, t)
        if m:
            hits.append((m.start(), float(m.group(1)) * f))
    if hits:                                   # the first unit stated wins, e.g. '500g (1.1 lbs)'
        v = min(hits)[1]
        return round(v, 3) if 0.01 <= v <= 100 else np.nan
    return np.nan

BRAND_ALIASES = {"hewlett": "hp", "hewlett-packard": "hp", "hp/compaq": "hp", "lg electronics": "lg",
                 "samsung electronics": "samsung", "fujifilm": "fujifilm", "fuji": "fujifilm",
                 "dell inc": "dell", "acer america": "acer", "asustek": "asus", "lenovo group": "lenovo"}

def canonical_brand(b):
    if not isinstance(b, str) or not b.strip():
        return np.nan
    b = re.sub(r"[^a-z0-9 \-/]", "", b.lower()).strip()
    if b in BRAND_ALIASES:
        return BRAND_ALIASES[b]
    first = b.split()[0] if b.split() else ""
    return BRAND_ALIASES.get(first, first) or np.nan

def parse_inches_in_title(title):
    """Screen size stated in a page title, e.g. 'Dell U2412M 24" Monitor' or '15.6-inch laptop'."""
    if not isinstance(title, str):
        return np.nan
    m = re.search(r'(\d{1,2}(?:\.\d)?)\s*(?:"|\'\'|”|-inch|\s?inch|in\b)', title.lower())
    return float(m.group(1)) if m and 1 <= float(m.group(1)) <= 100 else np.nan
