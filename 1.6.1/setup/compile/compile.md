# Compile Sedona source code

[![Scala and Java build](https://github.com/apache/sedona/actions/workflows/java.yml/badge.svg)](https://github.com/apache/sedona/actions/workflows/java.yml) [![Python build](https://github.com/apache/sedona/actions/workflows/python.yml/badge.svg)](https://github.com/apache/sedona/actions/workflows/python.yml) [![R build](https://github.com/apache/sedona/actions/workflows/r.yml/badge.svg)](https://github.com/apache/sedona/actions/workflows/r.yml) [![Example project build](https://github.com/apache/sedona/actions/workflows/example.yml/badge.svg)](https://github.com/apache/sedona/actions/workflows/example.yml) [![Docs build](https://github.com/apache/sedona/actions/workflows/docs.yml/badge.svg)](https://github.com/apache/sedona/actions/workflows/docs.yml) [![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/apache/sedona/HEAD?filepath=docs/usecases)

## Compile Scala / Java source code

Sedona Scala/Java code is a project with multiple modules. Each module is a Scala/Java mixed project which is managed by Apache Maven 3.

* Make sure your Linux/Mac machine has Java 1.8, Apache Maven 3.3.1+, and Python3.7+. The compilation of Sedona is not tested on Windows machines.

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
	By default, this command will compile Sedona with Spark 3.0 and Scala 2.12

### Compile with different targets

User can specify `-Dspark` and `-Dscala` command line options to compile with different targets. Available targets are:

* `-Dspark`: `3.0` for Spark 3.0 to 3.3; `{major}.{minor}` for Spark 3.4 or later. For example, specify `-Dspark=3.4` to build for Spark 3.4.
* `-Dscala`: `2.12` or `2.13`

=== "Spark 3.0 to 3.3 Scala 2.12"
	```
	mvn clean install -DskipTests -Dspark=3.0 -Dscala=2.12
	```
=== "Spark 3.4+ Scala 2.12"
	```
	mvn clean install -DskipTests -Dspark=3.4 -Dscala=2.12
	```
    Please replace `3.4` with Spark major.minor version when building for higher Spark versions.
=== "Spark 3.0 to 3.3 Scala 2.13"
	```
	mvn clean install -DskipTests -Dspark=3.0 -Dscala=2.13
	```
=== "Spark 3.4+ Scala 2.13"
	```
	mvn clean install -DskipTests -Dspark=3.4 -Dscala=2.13
	```
    Please replace `3.4` with Spark major.minor version when building for higher Spark versions.

!!!tip
	To get the Sedona Spark Shaded jar with all GeoTools jars included, simply append `-Dgeotools` option. The command is like this:`mvn clean install -DskipTests -Dscala=2.12 -Dspark=3.0 -Dgeotools`

### Download staged jars

Sedona uses GitHub Actions to automatically generate jars per commit. You can go [here](https://github.com/apache/sedona/actions/workflows/java.yml) and download the jars by clicking the commits ==Artifacts== tag.

## Run Python test

1. Set up the environment variable SPARK_HOME and PYTHONPATH

For example,

```
export SPARK_HOME=$PWD/spark-3.0.1-bin-hadoop2.7
export PYTHONPATH=$SPARK_HOME/python
```

2. Put JAI jars to ==SPARK_HOME/jars/== folder.

```
export JAI_CORE_VERSION="1.1.3"
export JAI_CODEC_VERSION="1.1.3"
export JAI_IMAGEIO_VERSION="1.1"
wget -P $SPARK_HOME/jars/ https://repo.osgeo.org/repository/release/javax/media/jai_core/${JAI_CORE_VERSION}/jai_core-${JAI_CORE_VERSION}.jar
wget -P $SPARK_HOME/jars/ https://repo.osgeo.org/repository/release/javax/media/jai_codec/${JAI_CODEC_VERSION}/jai_codec-${JAI_CODEC_VERSION}.jar
wget -P $SPARK_HOME/jars/ https://repo.osgeo.org/repository/release/javax/media/jai_imageio/${JAI_IMAGEIO_VERSION}/jai_imageio-${JAI_IMAGEIO_VERSION}.jar
```

3. Compile the Sedona Scala and Java code with `-Dgeotools` and then copy the ==sedona-spark-shaded-{{ sedona.current_version }}.jar== to ==SPARK_HOME/jars/== folder.

```
cp spark-shaded/target/sedona-spark-shaded-xxx.jar $SPARK_HOME/jars/
```

4. Install the following libraries

```
sudo apt-get -y install python3-pip python-dev libgeos-dev
sudo pip3 install -U setuptools
sudo pip3 install -U wheel
sudo pip3 install -U virtualenvwrapper
sudo pip3 install -U pipenv
```

Homebrew can be used to install libgeos-dev in macOS: `brew install geos`
5. Set up pipenv to the desired Python version: 3.7, 3.8, or 3.9

```
cd python
pipenv --python 3.7
```

6. Install the PySpark version and the other dependency

```
cd python
pipenv install pyspark
pipenv install --dev
```

`pipenv install pyspark` installs the latest version of pyspark.
In order to remain consistent with the installed spark version, use `pipenv install pyspark==<spark_version>`
7. Run the Python tests

```
cd python
pipenv run python setup.py build_ext --inplace
pipenv run pytest tests
```

## Compile the documentation

The website is automatically built after each commit. The built website can be downloaded here:

### MkDocs website

The source code of the documentation website is written in Markdown and then compiled by MkDocs. The website is built upon the [Material for MkDocs template](https://squidfunk.github.io/mkdocs-material/).

In the Sedona repository, the MkDocs configuration file ==mkdocs.yml== is in the root folder and all documentation source code is in docs folder.

To compile the source code and test the website on your local machine, please read the [MkDocs Tutorial](http://www.mkdocs.org/#installation) and [Materials for MkDocs Tutorial](https://squidfunk.github.io/mkdocs-material/getting-started/).

In short, you need to run:

```
pip install mkdocs
pip install mkdocs-jupyter
pip install mkdocs-material
pip install mkdocs-macros-plugin
pip install mkdocs-git-revision-date-localized-plugin
pip install mike
pip install pymdown-extensions
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

The hooks run when running `git commit`. Some of the hooks will auto fix the code after the hook fails
whilst most will print error messages from the linters.

If you want to test all hooks against all files and when you are adding a new hook
you should always run:

`pre-commit run --all-files`

Sometimes you might need to skip a hook to commit for example:

`SKIP=markdownlint git commit -m "foo"`

We have a [Makefile](https://github.com/apache/sedona/blob/master/Makefile) in the repository root which has three pre-commit convenience commands.
