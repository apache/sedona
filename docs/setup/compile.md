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

[![Scala and Java build](https://github.com/apache/sedona/actions/workflows/java.yml/badge.svg)](https://github.com/apache/sedona/actions/workflows/java.yml) [![Python build](https://github.com/apache/sedona/actions/workflows/python.yml/badge.svg)](https://github.com/apache/sedona/actions/workflows/python.yml) [![R build](https://github.com/apache/sedona/actions/workflows/r.yml/badge.svg)](https://github.com/apache/sedona/actions/workflows/r.yml) [![Example project build](https://github.com/apache/sedona/actions/workflows/example.yml/badge.svg)](https://github.com/apache/sedona/actions/workflows/example.yml) [![Docs build](https://github.com/apache/sedona/actions/workflows/docs.yml/badge.svg)](https://github.com/apache/sedona/actions/workflows/docs.yml)

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

Sedona's Python module now uses a modern `pyproject.toml` + `uv` workflow. Pipenv steps are deprecated (left only for historical reference). Below are the updated steps to build and run the Python tests against a local Spark installation.

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

4) Install system prerequisites (libgeos, build essentials)

Ubuntu / Debian:
```bash
sudo apt-get -y install python3-pip python3-dev libgeos-dev
```
macOS (Homebrew):
```bash
brew install geos
```

5) Create & activate a virtual environment using uv

```bash
cd python
python -m pip install --upgrade uv
uv venv --python 3.10   # or any supported version >=3.8
source .venv/bin/activate
```

6) Install Sedona (editable) with dev dependencies and a Spark version

```bash
uv pip install -e .[dev]
uv pip install pyspark==${SPARK_VERSION}
```
If you need all optional extras (maps, raster, etc.):
```bash
uv pip install -e .[dev,all]
```

7) (Optional) Rebuild the C extension explicitly (normally done automatically on install)
```bash
python setup.py build_ext --inplace
```

8) Run Python tests

Fast extension-only test:
```bash
pytest -v tests/utils/test_geomserde_speedup.py
```
Representative subset:
```bash
pytest -v tests/sql/test_aggregate_functions.py tests/utils/test_geomserde_speedup.py
```
All tests:
```bash
pytest -v tests
```

Notes:
- Keep SPARK_HOME exported so PySpark locates the correct distribution.
- To test different pyspark versions, reinstall with `uv pip install pyspark==<version> --reinstall`.
- For Spark Connect tests (Spark >= 3.4): `uv pip install "pyspark[connect]==${SPARK_VERSION}"` then run the relevant tests.

## Compile the documentation

The website is automatically built after each commit. The built website can be downloaded here:

### MkDocs website

The source code of the documentation website is written in Markdown and then compiled by MkDocs. The website is built upon the [Material for MkDocs template](https://squidfunk.github.io/mkdocs-material/).

In the Sedona repository, the MkDocs configuration file ==mkdocs.yml== is in the root folder and all documentation source code is in docs folder.

To compile the source code and test the website on your local machine, please read the [MkDocs Tutorial](http://www.mkdocs.org/#installation) and [Materials for MkDocs Tutorial](https://squidfunk.github.io/mkdocs-material/getting-started/).

In short, you need to run:

```
pip install -r requirements-docs.txt
```

After installing MkDocs and MkDocs-Material, run these commands in the Sedona root folder:

```
mkdocs build
mike deploy --update-aliases latest-snapshot -b website -p
mike serve
```

## pre-commit

We run [pre-commit](https://pre-commit.com/) with GitHub Actions so installation on
your local machine is currently optional.

The pre-commit [configuration file](https://github.com/apache/sedona/blob/master/.pre-commit-config.yaml)
is in the repository root. Before you can run the hooks, you need to have pre-commit installed.

The hooks run when running `git commit` and also from the command line with `pre-commit`. Some of the hooks will auto
fix the code after the hooks fail whilst most will print error messages from the linters. If a hook fails the overall
commit will fail, and you will need to fix the issues or problems and `git add` and git commit again. On git commit
the hooks will run mostly only against modified files so if you want to test all hooks against all files and when you
are adding a new hook you should always run:

`pre-commit run --all-files`

Sometimes you might need to skip a hook to commit because the hook is stopping you from committing or your computer
might not have all the installation requirements for all the hooks. The `SKIP` variable is comma separated for two or
more hooks:

`SKIP=codespell git commit -m "foo"`

The same applies when running pre-commit:

`SKIP=codespell pre-commit run --all-files`

Occasionally you can have more serious problems when using `pre-commit` with `git commit`. You can use `--no-verify` to
commit and stop `pre-commit` from checking the hooks. For example:

`git commit --no-verify -m "foo"`

If you just want to run one hook for example just run the `markdownlint` hook:

`pre-commit run markdownlint --all-files`

We have a [Makefile](https://github.com/apache/sedona/blob/master/Makefile) in the repository root which has three pre-commit convenience commands.

For example, you can run the following to setup pre-commit to run before each commit

```
make checkinstall
```
