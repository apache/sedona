<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
-->

# Sedona use-case notebooks

The notebooks at the **root** of this directory (`docs/usecases/*.ipynb`) are bundled into the Sedona docker image at `/opt/workspace/examples/` and rendered on the docs site via `mkdocs-jupyter`. The `legacy/` subdirectory holds older notebooks kept for backward-compatible URL access only — they are *not* shipped in the image. The `contrib/` subdirectory holds community-submitted examples that are also not shipped.

## Contract for shipped notebooks

Every notebook at the root of this directory **must**:

1. **Run end-to-end** in the docker image with default resources (`DRIVER_MEM=4g`, `EXECUTOR_MEM=4g`, `local[*]`). No external clusters, no GPUs.
2. **Finish in well under the 900-second per-notebook timeout** enforced by `docker/test-notebooks.sh`. Aim for under 2 minutes wall-clock on a laptop with a warm dataset cache.
3. **Use only data that ships in the image** (`docs/usecases/data/`) **or** is fetched from a public, anonymous-readable URL. No credentials, no private buckets.
4. **Tag network-dependent notebooks** so they can be skipped in sandboxed CI:

   ```markdown
   <!-- requires-network: true -->
   ```

   `docker/test-notebooks.sh` greps for that exact string and skips the notebook when `SEDONA_NOTEBOOK_OFFLINE=1` is set in the environment.
5. **Use `data/...` relative paths** for shipped data and `.master("spark://localhost:7077")` for the SedonaContext — the test harness rewrites both into the form needed for `local[*]` test mode (`examples/data/...`, `local[*]`).

## How CI verifies the contract

`.github/workflows/...` builds the docker image and runs:

```bash
docker run --rm sedona:dev /opt/sedona/docker/test-notebooks.sh
```

That script:

- iterates every `*.ipynb` at the root of `/opt/workspace/examples/`;
- for each, runs `jupyter nbconvert --to python --stdout`, applies sed rewrites
  (`spark://...` → `local[*]`, `data/...` → `examples/data/...`), strips IPython
  magics, then runs the resulting `.py` under `python3` with a 900s timeout and
  `set -o pipefail` so a notebook crash or timeout cannot be silently
  misreported as a pass;
- skips notebooks tagged `requires-network: true` when `SEDONA_NOTEBOOK_OFFLINE=1`;
- exits non-zero on any failure.

## How to verify a notebook locally before opening a PR

The fast feedback loop **is** the docker build:

```bash
# from the repo root
docker build -f docker/sedona-docker.dockerfile -t sedona:dev .
docker run --rm sedona:dev /opt/sedona/docker/test-notebooks.sh
docker run --rm -e SEDONA_NOTEBOOK_OFFLINE=1 sedona:dev /opt/sedona/docker/test-notebooks.sh
```

Both invocations must exit 0. The second variant proves your notebook either runs offline or is correctly tagged `requires-network: true`.

For faster iteration during notebook authoring, you can replicate the harness without a docker rebuild by running the notebook directly in a Python environment that matches the image's runtime:

- Python 3.10+ (image uses 3.12)
- `pyspark==4.0.1`
- `apache-sedona==1.9.0`
- `keplergl==0.3.7`, `geopandas`, `pyarrow`, `pandas`, `matplotlib`, `fiona==1.10.1`
- `JAVA_HOME` pointing at a JDK 17 install

Then export the Sedona Maven coordinates (the docker image bakes the JAR in directly; locally you let Maven pull it):

```bash
export PYSPARK_SUBMIT_ARGS="--packages org.apache.sedona:sedona-spark-shaded-4.0_2.13:1.9.0,org.datasyslab:geotools-wrapper:1.9.0-33.5 --driver-memory 4g pyspark-shell"
```

Convert and run:

```bash
jupyter nbconvert --to python docs/usecases/<your-notebook>.ipynb --stdout > /tmp/run.py
sed -i '' 's|\.master("spark://[^"]*")|.master("local[*]")|g' /tmp/run.py
( cd docs/usecases && python /tmp/run.py )
```

Successful runs match what `docker/test-notebooks.sh` will see in CI.

## Adding a new shipped notebook

1. Drop the `.ipynb` at the root of this directory.
2. If it needs new shipped data, add it under `data/` and verify total added bytes stay under ~25 MB unless there's a strong reason.
3. Verify it passes the contract above (network on AND off paths).
4. Open a PR with `[GH-####]` referencing the relevant issue.
