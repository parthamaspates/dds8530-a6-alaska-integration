"""Project settings. Every value can be overridden with an environment variable."""
import os
from pathlib import Path

ROOT = Path(os.getenv("PROJECT_ROOT", Path(__file__).resolve().parents[2]))
DATA_DIR = Path(os.getenv("DATA_DIR", ROOT / "data"))
RAW_DIR = DATA_DIR / "raw"                 # extracted Alaska archives
SOURCES_DIR = DATA_DIR / "sources"         # CSV feed and streaming hold-out files
OUT_DIR = Path(os.getenv("OUT_DIR", ROOT / "outputs"))
FIG_DIR = OUT_DIR / "figures"

VERTICALS = ["camera", "monitor", "notebook"]
ALASKA_URL = "https://raw.githubusercontent.com/merialdo/research.alaska/main/{v}.tar.gz"

# Heterogeneous storage: each vertical lives in a different system.
STORE_OF = {"camera": "postgresql", "monitor": "mongodb", "notebook": "csv"}
# One web source per vertical is held out and arrives later as a live feed through Kafka.
STREAM_SOURCES = {
    "camera": "www.priceme.co.nz",
    "monitor": "www.makingbuyingeasy.co.uk",
    "notebook": "www.amazon.com",
}

PG_URL = os.getenv("PG_URL", "postgresql+psycopg2://alaska:alaska@localhost:5432/alaska")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "alaska")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "alaska.listings")
FEED_API_URL = os.getenv("FEED_API_URL", "http://localhost:8000")
FEED_RATE = float(os.getenv("FEED_RATE", "25"))          # listings released per second
SPARK_MASTER = os.getenv("SPARK_MASTER", "local[*]")
SPARK_KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"

for p in (RAW_DIR, SOURCES_DIR, FIG_DIR):
    p.mkdir(parents=True, exist_ok=True)
