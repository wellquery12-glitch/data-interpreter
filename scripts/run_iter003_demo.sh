#!/usr/bin/env bash
set -euo pipefail

SRC="${1:-storage/testdata/demo.csv}"
RUN_NAME="${2:-iter003_demo}"
export SRC_VALUE="${SRC}"
export RUN_NAME_VALUE="${RUN_NAME}"

python3 - <<'PY'
import json
import os
from pipelines import UciAutoPipeline

source = os.environ.get("SRC_VALUE", "storage/testdata/demo.csv")
run_name = os.environ.get("RUN_NAME_VALUE", "iter003_demo")

pipe = UciAutoPipeline(datasets_dir="datasets", outputs_dir="outputs")
out = pipe.run(source=source, run_name=run_name)
print(json.dumps(out, ensure_ascii=False, indent=2))
PY
