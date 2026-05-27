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

# Compile Sedona source code

## Compile Scala / Java source code

Sedona Scala/Java code is a project with multiple modules. Each module is a Scala/Java mixed project which is managed by Apache Maven 3.

* Make sure your Linux/Mac machine has Java 11/17, Apache Maven 3.3.1+, and Python3.8+. The compilation of Sedona is not tested on Windows machines.

To compile all modules, please make sure you are in the root folder of all modules. Then enter the following command in the terminal:

=== "Without unit tests"
	```bash
	mvn clean install -DskipTests
	```
	This command will first delete the old binary files and compile all modules. This compilation will skip the unit tests. To compile a single module, please make sure you are in the folder of that module. Then enter the same command.

=== "With unit tests"
	```bash
	mvn clean install
	```
	The maven unit tests of all modules may take up to 30 minutes.

=== "With Geotools jars packaged"
	```bash
	mvn clean install -DskipTests -Dgeotools
	```
	Geotools jars will be packaged into the produced fat jars.

!!!note
	By default, this command will compile Sedona with Spark 3.4 and Scala 2.12

### Compile with different targets

User can specify `-Dspark` and `-Dscala` command line options to compile with different targets. Available targets are:

* `-Dspark`: `{major}.{minor}`: For example, specify `-Dspark=3.4` to build for Spark 3.4.
* `-Dscala`: `2.12` or `2.13`

=== "Spark 3.4+ Scala 2.12"
	```
	mvn clean install -DskipTests -Dspark=3.4 -Dscala=2.12
	```
    Please replace `3.4` with Spark major.minor version when building for higher Spark versions.
=== "Spark 3.4+ Scala 2.13"
	```
	mvn clean install -DskipTests -Dspark=3.4 -Dscala=2.13
	```
    Please replace `3.4` with Spark major.minor version when building for higher Spark versions.

!!!tip
	To get the Sedona Spark Shaded jar with all GeoTools jars included, simply append `-Dgeotools` option. The command is like this:`mvn clean install -DskipTests -Dscala=2.12 -Dspark=3.4 -Dgeotools`

### Download staged jars

Sedona uses GitHub Actions to automatically generate jars per commit. You can go [here](https://github.com/apache/sedona/actions/workflows/java.yml) and download the jars by clicking the commits ==Artifacts== tag.

## Run Python test

1) Set up Spark (download if needed) and environment variables

```bash
export SPARK_VERSION=3.4.0   # or another supported version
wget https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz
tar -xvzf spark-${SPARK_VERSION}-bin-hadoop3.tgz
rm spark-${SPARK_VERSION}-bin-hadoop3.tgz
export SPARK_HOME=$PWD/spark-${SPARK_VERSION}-bin-hadoop3
export PYTHONPATH=$SPARK_HOME/python
```

2) Add required JAI jars into $SPARK_HOME/jars

```bash
export JAI_CORE_VERSION="1.1.3"
export JAI_CODEC_VERSION="1.1.3"
export JAI_IMAGEIO_VERSION="1.1"
wget -P $SPARK_HOME/jars/ https://repo.osgeo.org/repository/release/javax/media/jai_core/${JAI_CORE_VERSION}/jai_core-${JAI_CORE_VERSION}.jar
wget -P $SPARK_HOME/jars/ https://repo.osgeo.org/repository/release/javax/media/jai_codec/${JAI_CODEC_VERSION}/jai_codec-${JAI_CODEC_VERSION}.jar
wget -P $SPARK_HOME/jars/ https://repo.osgeo.org/repository/release/javax/media/jai_imageio/${JAI_IMAGEIO_VERSION}/jai_imageio-${JAI_IMAGEIO_VERSION}.jar
```

3) Build Sedona Scala/Java jars with GeoTools shaded (from repo root)

```bash
mvn clean install -DskipTests -Dgeotools
cp spark-shaded/target/sedona-spark-shaded-*.jar $SPARK_HOME/jars/
```

4) Setup Python development environment

The Python package uses `pyproject.toml` (PEP 517/518) with setuptools as the build backend. We recommend using [uv](https://docs.astral.sh/uv/) to manage virtual environments and dependencies.

```bash
cd python
python -m pip install --upgrade uv
uv venv --python 3.10   # or any supported version (>=3.8)
```

5) Install the PySpark version and the other dependency

```bash
cd python
# Use the correct PySpark version, otherwise latest version will be installed
uv add pyspark==${SPARK_VERSION} --optional spark
uv sync
```

6) Install Sedona (editable) and run the Python tests

```bash
cd python
uv pip install -e .
uv run pytest -v tests
```

## Compile the documentation

The website is automatically built after each commit. The built website can be downloaded here:

### MkDocs website

The source code of the documentation website is written in Markdown and then compiled by MkDocs. The website is built upon the [Material for MkDocs template](https://squidfunk.github.io/mkdocs-material/).

In the Sedona repository, the MkDocs configuration file ==mkdocs.yml== is in the root folder and all documentation source code is in docs folder.

To compile the source code and test the website on your local machine, please read the [MkDocs Tutorial](http://www.mkdocs.org/#installation) and [Materials for MkDocs Tutorial](https://squidfunk.github.io/mkdocs-material/getting-started/).

In short, you need to run:

```
python3 -m pip install uv
uv sync --group docs
```

After installing MkDocs and MkDocs-Material, run these commands in the Sedona root folder:

```
cd docs-overrides && npm ci && npx gulp build
cd ..
uv run mike deploy --update-aliases latest-snapshot -b website -p
uv run mike serve -b website
```

## Code Quality Checks with `prek`, `uv`, and `make`

This guide explains how Apache Sedona manages local code quality checks, formatting, and linting. To ensure a fast and consistent developer experience, we combine **`uv`** (a lightning-fast Python package and environment manager) with **`prek`** (our internal pre-commit wrapper framework) and expose everything via a simple, unified `Makefile`.

---

### Prerequisites

Before running any checks, ensure you have `uv` installed on your machine. If you don't have it yet, you can install it via the official installer:

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

> 💡 **Note:** You do not need to manually install Python virtual environments, `pre-commit`, or `prek`. The `Makefile` handles environment isolation and dependency resolution automatically using `uv`.

---

### The Core Workflow: `uv` + `prek`

Our codebase uses `prek` to orchestrate linting and formatting workflows (such as *Black*, *markdownlint*, or *oxipng*).

Instead of forcing you to activate a virtual environment manually, the `Makefile` uses `uv sync` to build a perfectly isolated environment behind the scenes. When you run any `make check-*` command, it guarantees that `prek` is executed using the exact dependency versions locked in the project.

---

### Available Make Commands

For maximum convenience, you should interact with `prek` entirely through the following `make` shortcuts:

* **`make check-stage`** *Runs checks on staged changes only (Recommended before committing).* Evaluates `prek` only against the files you have explicitly staged using `git add`. It is incredibly fast.
* **`make check`** *Runs checks on the full codebase.* Forces `prek` to evaluate every single file in the repository, regardless of git status. **Use this before opening a Pull Request.**
* **`make check-from-ref`** *Runs checks on your entire branch history.* Automatically identifies all files changed since you branched off of the `main` branch and runs linting rules against them.
* **`make check-last`** *Runs checks on your last commit.* If you just committed code and want to double-check that everything passes post-commit.
* **`make update-deps`** *Keeps linter hooks up to date.* Upgrades `prek` hooks to their latest respective upstream versions and updates your local locked dependencies.

---

### Advanced Usage & Examples

#### Skipping Specific Hooks Locally

Sometimes a specific linter rule blocks your workflow, or you want to isolate your testing to a single hook. You can bypass specific hooks by passing the `SKIP` environment variable directly to `prek`.

* **Example:** Run all checks except the linter named `codespell`

```bash
SKIP=codespell make check-stage
```

* **Example:** Skip multiple hooks simultaneously (comma-separated)

```bash
SKIP=codespell,gitleaks make check
```

#### Bypassing Automated Git Hooks (`--no-verify`)

If you have configured `prek` to run automatically as a native Git `pre-commit` hook on your system, it will block `git commit` if any files fail the quality gates.

If you need to commit a work-in-progress (WIP) branch quickly without fixing style errors, you can bypass the `prek` gate entirely using Git's built-in `--no-verify` (or `-n`) flag.

```bash
git add .
git commit -m "WIP: basic structure for new feature" --no-verify
```

> ⚠️ **Warning:** Continuous Integration (CI) runners will still execute all `prek` gates on your Pull Request. Bypassing hooks with `--no-verify` should only be used for temporary local savepoints.

---

### Housekeeping & Resetting Cache (`make clean`)

If your Python environment behaves unexpectedly, or if the linters fail to pick up recent changes due to aggressive local caching, you should reset your workspace back to a pristine state using:

```bash
make clean
```

#### What happens under the hood?

This command triggers a multi-directory sweep to remove compiled local artifacts and temporary caches:

* `__pycache__`: Clears out compiled Python bytecode files (`.pyc`).
* `.mypy_cache`: Empties the type-checking state cache to ensure a fresh, accurate type analysis next run.
* `.pytest_cache`: Cleans up previous test failure tracking states.

Following a `make clean`, running any subsequent `make check` command will completely rebuild the `uv` virtual environment mapping and rerun all `prek` checks completely fresh.

---

### Architecture Pipeline

When you execute a command like `make check`, the automation engine follows these exact steps:

1. **`check-install`**: Verifies `uv` is installed on your local path.
2. **`install`**: Triggers `uv sync --all-groups`, which automatically creates/syncs a `.venv` folder containing `prek` and all development dependencies.
3. **`Execution`**: Runs the target `prek` command within that guaranteed environment context.

```
[ Your CLI ] ──> [ make check ] ──> [ uv syncs environment ] ──> [ prek executes hooks ]
```
