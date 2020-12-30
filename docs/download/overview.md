## Supported platforms

Sedona supports Spark 2.4 and 3.0. Sedona Scala/Java/Python also work with Spark 2.3 but we have no plan to officially support it.

Sedona is compiled with Java 1.8 and Scala 2.11/2.12:

|             | Spark 2.4 | Spark 3.0 |
|:-----------:| :---------:|:---------:|
| Scala 2.11  |  ✅  |  not tested  |
| Scala 2.12 | ✅  |  ✅  |

Sedona Python is tested on the following Python and Spark verisons:

|             | Spark 2.4 (Scala 2.11) | Spark 3.0 (Scala 2.12)|
|:-----------:|:---------:|:---------:|
| Python 3.7  |  ✅  |  ✅  |
| Python 3.8 | not tested  |  ✅  |
| Python 3.9 | not tested  |  ✅  |


## Direct download

Sedona source code is hosted on [GitHub repository](https://github.com/apache/incubator-sedona/).

Sedona pre-compiled JARs are hosted on [GitHub Releases](https://github.com/apache/incubator-sedona/releases).

Sedona pre-compiled JARs are hosted on [Maven Central](../GeoSpark-All-Modules-Maven-Central-Coordinates).

Sedona automatically staged JARs (per each Master branch commit) are hosted by [GitHub Action](https://github.com/apache/incubator-sedona/actions?query=workflow%3A%22Scala+and+Java+build%22).

Sedona release notes are here [Release notes](../GeoSpark-All-Modules-Release-notes).

## Use Sedona Scala/Java

Before starting the Sedona journey, you need to make sure your Apache Spark cluster is ready.

There are two ways to use a Scala or Java library with Apache Spark. You can user either one to run Sedona.

* [Spark interactive Scala shell](../scalashell): easy to start, good for new learners to try simple functions
* [Self-contained Scala / Java project](../project): a steep learning curve of package management, but good for large projects

## Install Sedona Python

Apache Sedona extends pyspark functions which depends on libraries:

* pyspark
* shapely
* attrs

### Install sedona

* Installing from PyPi repositories

```bash
pip install sedona
```

* Installing from source

Clone Sedona GitHub source code and run the following command

```bash
cd python-adapter
python3 setup.py install
```

### Prepare python-adapter jar

Sedona Python needs one additional jar file call `sedona-python-adapter-3.0_2.12-1.0.0-incubator.jar` to work properly. Please make sure you use the correct version for Spark and Scala.

You can get it using the following methods:

* Compile from the source within main project directory and copy it (in `target` folder) to SPARK_HOME/jars/ folder ([more details](/download/compile/#compile-scala-and-java-source-code))

* Download from [GitHub release](https://github.com/apache/incubator-sedona/releases) and copy it to SPARK_HOME/jars/ folder
* Call the [Maven Central coordinate](../GeoSpark-All-Modules-Maven-Central-Coordinates) in your python program. For example, in PySparkSQL
```python
    spark = SparkSession.\
        builder.\
        appName('appName').\
        config("spark.serializer", KryoSerializer.getName).\
        config("spark.kryo.registrator", SedonaKryoRegistrator.getName) .\
        config('spark.jars.packages', 'org.apache.sedona:sedona-python-adapter-3.0_2.12:1.0.0-incubator').\
        getOrCreate()
```