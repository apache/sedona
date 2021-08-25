## Supported platforms

Sedona supports Spark 2.4 and 3.0, Python 3.7 - 3.9. Sedona Scala/Java/Python also work with Spark 2.3, Python 3.6 but we have no plan to officially support it.

Sedona is compiled and tested on Java 1.8 and Scala 2.11/2.12:

|             | Spark 2.4 | Spark 3.0+ |
|:-----------:| :---------:|:---------:|
| Scala 2.11  |  ✅  |  not tested  |
| Scala 2.12 | ✅  |  ✅  |

Sedona Python is tested on the following Python and Spark verisons:

|             | Spark 2.4 (Scala 2.11) | Spark 3.0+ (Scala 2.12)|
|:-----------:|:---------:|:---------:|
| Python 3.7  |  ✅  |  ✅  |
| Python 3.8 | not tested  |  ✅  |
| Python 3.9 | not tested  |  ✅  |


## Direct download

[Release notes](../release-notes)

Latest source code: [GitHub repository](https://github.com/apache/incubator-sedona/).

Release source code and binary jars: [GitHub releases](https://github.com/apache/incubator-sedona/releases), [Maven Central](../maven-coordinates).

Automatically generated binary JARs (per each Master branch commit): [GitHub Action](https://github.com/apache/incubator-sedona/actions?query=workflow%3A%22Scala+and+Java+build%22).

## Use Sedona Scala/Java

Before starting the Sedona journey, you need to make sure your Apache Spark cluster is ready.

There are two ways to use a Scala or Java library with Apache Spark. You can user either one to run Sedona.

* [Spark interactive Scala shell](../scalashell): easy to start, good for new learners to try simple functions
* [Self-contained Scala / Java project](../project): a steep learning curve of package management, but good for large projects

## Install Sedona Python

Click [![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/apache/incubator-sedona/HEAD?filepath=binder) and play the interactive Sedona Python Jupyter Notebook immediately!

Apache Sedona extends pyspark functions which depends on libraries:

* pyspark
* shapely
* attrs

You need to install necessary packages if your system does not have them installed. See ["packages" in our Pipfile](https://github.com/apache/incubator-sedona/blob/master/python/Pipfile).

### Install sedona

* Installing from PyPi repositories. You can find the latest Sedona Python on [PyPi](https://pypi.org/project/apache-sedona/). [There is an known issue in Sedona v1.0.1 and earlier versions](../release-notes/#known-issue).

```bash
pip install apache-sedona
```
* Since version 1.1.0 pyspark is an optional dependency since spark comes pre-installed on many spark platforms.
  To install pyspark along with Sedona Python in one go, use the `spark` extra:
```bash
pip install apache-sedona[spark]
```

* Installing from Sedona Python source

Clone Sedona GitHub source code and run the following command

```bash
cd python
python3 setup.py install
```

### Prepare python-adapter jar

Sedona Python needs one additional jar file called `sedona-python-adapter` to work properly. Please make sure you use the correct version for Spark and Scala. For Spark 3.0 + Scala 2.12, it is called `sedona-python-adapter-3.0_2.12-1.0.1-incubating.jar`

You can get it using one of the following methods:

1. Compile from the source within main project directory and copy it (in `python-adapter/target` folder) to SPARK_HOME/jars/ folder ([more details](/download/compile/#compile-scala-and-java-source-code))

2. Download from [GitHub release](https://github.com/apache/incubator-sedona/releases) and copy it to SPARK_HOME/jars/ folder
3. Call the [Maven Central coordinate](../maven-coordiantes) in your python program. For example, in PySparkSQL
```python
spark = SparkSession. \
    builder. \
    appName('appName'). \
    config("spark.serializer", KryoSerializer.getName). \
    config("spark.kryo.registrator", SedonaKryoRegistrator.getName). \
    config('spark.jars.packages',
           'org.apache.sedona:sedona-python-adapter-3.0_2.12:1.0.1-incubating,'
           'org.datasyslab:geotools-wrapper:geotools-24.1'). \
    getOrCreate()
```

!!!warning
	If you are going to use Sedona CRS transformation and ShapefileReader functions, you have to use Method 1 or 3. Because these functions internally use GeoTools libraries which are under LGPL license, Apache Sedona binary release cannot include them.

### Setup environment variables

If you manually copy the python-adapter jar to `SPARK_HOME/jars/` folder, you need to setup two environment variables

* SPARK_HOME. For example, run the command in your terminal

```bash
export SPARK_HOME=~/Downloads/spark-3.0.1-bin-hadoop2.7
```

* PYTHONPATH. For example, run the command in your terminal

```bash
export PYTHONPATH=$SPARK_HOME/python
``` 

You can then play with [Sedona Python Jupyter notebook](/tutorial/jupyter-notebook/).