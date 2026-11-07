import math
import pandas as pd
from alaska_int import clean, er, schema_match

def test_parse_inches():
    assert clean.parse_inches("21.5''") == 21.5
    assert clean.parse_inches("15.6 inches") == 15.6
    assert abs(clean.parse_inches("39.6 cm") - 15.6) < 0.1
    assert math.isnan(clean.parse_inches("n/a"))

def test_parse_kg():
    assert clean.parse_kg("2.1 kg") == 2.1
    assert abs(clean.parse_kg("8 pounds") - 3.629) < 0.01
    assert clean.parse_kg("500g (1.1 lbs)") == 0.5

def test_canonical_brand():
    assert clean.canonical_brand("Hewlett-Packard") == "hp"
    assert clean.canonical_brand("  ASUS ") == "asus"

def test_clean_long_removes_placeholders_and_duplicates():
    df = pd.DataFrame({"spec_id": ["a", "a", "a", "b", "b", "c", "c"], "vertical": "monitor",
                       "source": ["s"] * 7, "attribute": ["Brand:", "brand", "Color", "brand", "color", "brand", "color"],
                       "value": ["Dell", "Dell", "N/A", "Dell", "Black", "Dell", "Black"]})
    out, rep = clean.clean_long(df)
    assert rep["placeholder_values_removed"] == 1
    assert rep["duplicate_attribute_rows"] == 1
    assert rep["duplicate_listings_removed"] == 1          # c duplicates b in the same source
    assert set(out.spec_id) == {"a", "b"}

def test_model_codes_skip_units():
    codes = er.model_codes({"<page title>": "Dell U2412M 24in 1920x1080 LED Monitor 2014", "mpn": "U2412M"})
    assert "U2412M" in codes and "1920X1080" not in codes and "2014" not in codes

def test_er_and_evaluation():
    rows = []
    for sid, t in [("x:1", "Sony ILCE-7R body"), ("x:2", "Sony Alpha ILCE 7R"), ("x:3", "Canon EOS 70D kit")]:
        rows += [(sid, "camera", "s", "<page title>", t), (sid, "camera", "s", "brand", t.split()[0])]
    long_df = pd.DataFrame(rows, columns=["spec_id", "vertical", "source", "attribute", "value"])
    resolved, vocab, _ = er.resolve_pandas(long_df.assign(attribute=long_df.attribute))
    assert resolved.set_index("spec_id").entity_id["x:1"] == resolved.set_index("spec_id").entity_id["x:2"]
    gt = pd.DataFrame({"vertical": "camera", "entity_id": ["E1", "E1", "E2"], "spec_id": ["x:1", "x:2", "x:3"]})
    ev = er.evaluate(resolved, gt)
    assert ev.f1.iat[0] == 1.0

def test_schema_match_synonyms():
    long_df = pd.DataFrame({"spec_id": ["a"] * 2, "vertical": "monitor", "source": "s",
                            "attribute": ["manufacturer", "screen size"], "value": ["Dell", "24in"]})
    pred = schema_match.match(long_df, {"monitor": ["brand", "screen_size_diagonal", "response_time"]})
    got = dict(zip(pred.attribute, pred.target))
    assert got["manufacturer"] == "brand" and got["screen size"] == "screen_size_diagonal"
