# Compile Sedona source code

[![Scala and Java build](https://github.com/apache/sedona/actions/workflows/java.yml/badge.svg)](https://github.com/apache/sedona/actions/workflows/java.yml) [![Python build](https://github.com/apache/sedona/actions/workflows/python.yml/badge.svg)](https://github.com/apache/sedona/actions/workflows/python.yml) [![R build](https://github.com/apache/sedona/actions/workflows/r.yml/badge.svg)](https://github.com/apache/sedona/actions/workflows/r.yml) [![Example project build](https://github.com/apache/sedona/actions/workflows/example.yml/badge.svg)](https://github.com/apache/sedona/actions/workflows/example.yml) [![Docs build](https://github.com/apache/sedona/actions/workflows/docs.yml/badge.svg)](https://github.com/apache/sedona/actions/workflows/docs.yml) [![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/apache/sedona/HEAD?filepath=binder)


## Compile Scala / Java source code
Sedona Scala/Java code is a project with multiple modules. Each module is a Scala/Java mixed project which is managed by Apache Maven 3.

* Make sure your Linux/Mac machine has Java 1.8, Apache Maven 3.3.1+, and Python3. The compilation of Sedona is not tested on Windows machine.

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

=== "Spark 3.0 + Scala 2.12"
	```
	mvn clean install -DskipTests -Dscala=2.12
	```
=== "Spark 3.0 + Scala 2.13"
	```
	mvn clean install -DskipTests -Dscala=2.13
	```

!!!tip
	To get the Sedona Spark Shaded jar with all GeoTools jars included, simply append `-Dgeotools` option. The command is like this:`mvn clean install -DskipTests -Dscala=2.12 -Dspark=3.0 -Dgeotools`

### Download staged jars

Sedona uses GitHub action to automatically generate jars per commit. You can go [here](https://github.com/apache/sedona/actions/workflows/java.yml) and download the jars by clicking the commit's ==Artifacts== tag.

## Run Python test

1. Set up the environment variable SPARK_HOME and PYTHONPATH

For example,
```
export SPARK_HOME=$PWD/spark-3.0.1-bin-hadoop2.7
export PYTHONPATH=$SPARK_HOME/python
```
2. Compile the Sedona Scala and Java code with `-Dgeotools` and then copy the ==sedona-spark-shaded-{{ sedona.current_version }}.jar== to ==SPARK_HOME/jars/== folder.
```
cp spark-shaded/target/sedona-spark-shaded-xxx.jar SPARK_HOME/jars/
```
3. Install the following libraries
```
sudo apt-get -y install python3-pip python-dev libgeos-dev
sudo pip3 install -U setuptools
sudo pip3 install -U wheel
sudo pip3 install -U virtualenvwrapper
sudo pip3 install -U pipenv
```
4. Set up pipenv to the desired Python version: 3.7, 3.8, or 3.9
```
cd python
pipenv --python 3.7
```
5. Install the PySpark version and other dependency
```
cd python
pipenv install pyspark==3.0.1
pipenv install --dev
```
6. Run the Python tests
```
cd python
pipenv run pytest tests
```
## Compile the documentation

The website is automatically built after each commit. The built website can be downloaded here: 

### MkDocs website

The source code of the documentation website is written in Markdown and then compiled by MkDocs. The website is built upon [Material for MkDocs template](https://squidfunk.github.io/mkdocs-material/).

In the Sedona repository, MkDocs configuration file ==mkdocs.yml== is in the root folder and all documentation source code is in docs folder.

To compile the source code and test the website on your local machine, please read [MkDocs Tutorial](http://www.mkdocs.org/#installation) and [Materials for MkDocs Tutorial](https://squidfunk.github.io/mkdocs-material/getting-started/).

In short, you need to run:

```
pip install mkdocs
pip install mkdocs-material
pip install mkdocs-macros-plugin
pip install mkdocs-git-revision-date-localized-plugin
pip install mike
```

After installing MkDocs and MkDocs-Material, run the command in Sedona root folder:

```
mike deploy --update-aliases latest-snapshot latest
mike set-default latest
mike serve
```

