Click [![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/apache/sedona/HEAD?filepath=binder) and play the interactive Sedona Python Jupyter Notebook immediately!

Apache Sedona extends pyspark functions which depends on libraries:

* pyspark
* shapely
* attrs

You need to install necessary packages if your system does not have them installed. See ["packages" in our Pipfile](https://github.com/apache/sedona/blob/master/python/Pipfile).

### Install sedona

* Installing from PyPI repositories. You can find the latest Sedona Python on [PyPI](https://pypi.org/project/apache-sedona/). [There is an known issue in Sedona v1.0.1 and earlier versions](../release-notes/#known-issue).

```bash
pip install apache-sedona
```

* Since Sedona v1.1.0, pyspark is an optional dependency of Sedona Python because spark comes pre-installed on many spark platforms. To install pyspark along with Sedona Python in one go, use the `spark` extra:
  
```bash
pip install apache-sedona[spark]
```

* Installing from Sedona Python source

Clone Sedona GitHub source code and run the following command

```bash
cd python
python3 setup.py install
```

### Prepare sedona-spark-shaded jar

Sedona Python needs one additional jar file called `sedona-spark-shaded` to work properly. Please make sure you use the correct version for Spark and Scala. For Spark 3.0 + Scala 2.12, it is called `sedona-spark-shaded-3.0_2.12-{{ sedona.current_version }}.jar`

You can get it using one of the following methods:

1. Compile from the source within main project directory and copy it (in `spark-shaded/target` folder) to SPARK_HOME/jars/ folder ([more details](../compile))

2. Download from [GitHub release](https://github.com/apache/sedona/releases) and copy it to SPARK_HOME/jars/ folder
3. Call the [Maven Central coordinate](../maven-coordinates) in your python program. For example, in PySparkSQL
```python
spark = SparkSession. \
    builder. \
    appName('appName'). \
    config("spark.serializer", KryoSerializer.getName). \
    config("spark.kryo.registrator", SedonaKryoRegistrator.getName). \
    config('spark.jars.packages',
           'org.apache.sedona:sedona-spark-shaded-3.0_2.12:{{ sedona.current_version }},'
           'org.datasyslab:geotools-wrapper:{{ sedona.current_geotools }}'). \
    getOrCreate()
```

!!!warning
	If you are going to use Sedona CRS transformation and ShapefileReader functions, you have to use Method 1 or 3. Because these functions internally use GeoTools libraries which are under LGPL license, Apache Sedona binary release cannot include them.

### Setup environment variables

If you manually copy the sedona-spark-shaded jar to `SPARK_HOME/jars/` folder, you need to setup two environment variables

* SPARK_HOME. For example, run the command in your terminal

```bash
export SPARK_HOME=~/Downloads/spark-3.0.1-bin-hadoop2.7
```

* PYTHONPATH. For example, run the command in your terminal

```bash
export PYTHONPATH=$SPARK_HOME/python
```

You can then play with [Sedona Python Jupyter notebook](../../tutorial/jupyter-notebook/).