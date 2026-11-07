#!/usr/bin/env bash
# Runs inside the app container: data download, tests, notebook execution, HTML export.
set -euo pipefail
cd /work
mkdir -p outputs
exec > >(tee outputs/run_all.log) 2>&1
echo "== $(date -u) start"
python scripts/get_data.py
pytest -q tests | tee outputs/tests.txt
python scripts/build_notebook.py
jupyter nbconvert --to notebook --execute --inplace notebooks/A6_Alaska_Integration.ipynb --ExecutePreprocessor.timeout=3600
jupyter nbconvert --to html notebooks/A6_Alaska_Integration.ipynb --output-dir outputs
echo "== $(date -u) DONE"
