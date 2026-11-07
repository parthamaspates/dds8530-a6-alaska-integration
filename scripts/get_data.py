"""Download and extract the three Alaska verticals (camera, monitor, notebook) into data/raw."""
import sys, tarfile, urllib.request
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from alaska_int import config

for v in config.VERTICALS:
    target = config.RAW_DIR / v
    if (target / f"{v}_specs").exists():
        print(f"{v}: already present")
        continue
    archive = config.RAW_DIR / f"{v}.tar.gz"
    print(f"{v}: downloading {config.ALASKA_URL.format(v=v)}")
    urllib.request.urlretrieve(config.ALASKA_URL.format(v=v), archive)
    with tarfile.open(archive) as tar:
        tar.extractall(config.RAW_DIR, filter="data")
    archive.unlink()
    print(f"{v}: extracted")
