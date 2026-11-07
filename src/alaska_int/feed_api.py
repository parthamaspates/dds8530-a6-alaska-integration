"""Marketplace listing API (FastAPI). Replays the held-out sources as if listings were being published live:
listing i becomes visible i / FEED_RATE seconds after the first request."""
import json, random, time
from fastapi import FastAPI, Query
from . import config

app = FastAPI(title="Alaska live listing feed")
_state = {"listings": None, "t0": None}

def _load():
    if _state["listings"] is None:
        path = config.SOURCES_DIR / "stream_listings.jsonl"
        items = [json.loads(l) for l in open(path)]
        random.Random(8530).shuffle(items)          # interleave the three sources
        _state["listings"] = items
    return _state["listings"]

@app.get("/health")
def health():
    return {"status": "ok"}

@app.post("/reset")
def reset():
    _state["listings"], _state["t0"] = None, None
    return {"status": "reset"}

@app.get("/listings")
def listings(offset: int = Query(0, ge=0), limit: int = Query(200, le=1000)):
    items = _load()
    if _state["t0"] is None:
        _state["t0"] = time.time()
    available = min(len(items), int((time.time() - _state["t0"]) * config.FEED_RATE))
    batch = items[offset:min(available, offset + limit)]
    return {"offset": offset, "count": len(batch), "available": available, "total": len(items),
            "done": offset + len(batch) >= len(items), "listings": batch}
