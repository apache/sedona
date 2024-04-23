Click [![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/apache/sedona/HEAD?filepath=docs/usecases) and play the interactive Sedona Python Jupyter Notebook immediately!

Apache Sedona extends pyspark functions which depends on libraries:

* pyspark
* shapely
* attrs

You need to install necessary packages if your system does not have them installed. See ["packages" in our Pipfile](https://github.com/apache/sedona/blob/master/python/Pipfile).

### Install sedona

* Installing from PyPI repositories. You can find the latest Sedona Python on [PyPI](https://pypi.org/project/apache-sedona/). [There is a known issue in Sedona v1.0.1 and earlier versions](release-notes.md#known-issue).

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

### Prepare sedona-spark jar

Sedona Python needs one additional jar file called `sedona-spark-shaded` or `sedona-spark` to work properly. Please make sure you use the correct version for Spark and Scala.

* For Spark 3.0 to 3.3 and Scala 2.12, it is called `sedona-spark-shaded-3.0_2.12-{{ sedona.current_version }}.jar` or `sedona-spark-3.0_2.12-{{ sedona.current_version }}.jar`
* For Spark 3.4+ and Scala 2.12, it is called `sedona-spark-shaded-3.4_2.12-{{ sedona.current_version }}.jar` or `sedona-spark-3.4_2.12-{{ sedona.current_version }}.jar`. If you are using Spark versions higher than 3.4, please replace the `3.4` in artifact names with the corresponding major.minor version numbers.

You can get it using one of the following methods:

1. If you run Sedona in Databricks, AWS EMR, or other cloud platform's notebook, use the `shaded jar`: Download [sedona-spark-shaded jar](https://repo.maven.apache.org/maven2/org/apache/sedona/) and [geotools-wrapper jar](https://repo.maven.apache.org/maven2/org/datasyslab/geotools-wrapper/) from Maven Central, and put them in SPARK_HOME/jars/ folder.
2. If you run Sedona in an IDE or a local Jupyter notebook, use the `unshaded jar`. Call the [Maven Central coordinate](maven-coordinates.md) in your python program. For example,
==Sedona >= 1.4.1==

```python
from sedona.spark import *
config = SedonaContext.builder(). \
    config('spark.jars.packages',
           'org.apache.sedona:sedona-spark-3.0_2.12:{{ sedona.current_version }},'
           'org.datasyslab:geotools-wrapper:{{ sedona.current_geotools }}'). \
    config('spark.jars.repositories', 'https://artifacts.unidata.ucar.edu/repository/unidata-all'). \
    getOrCreate()
sedona = SedonaContext.create(config)
```

==Sedona < 1.4.1==

SedonaRegistrator is deprecated in Sedona 1.4.1 and later versions. Please use the above method instead.

```python
from pyspark.sql import SparkSession
from sedona.register import SedonaRegistrator
from sedona.utils import SedonaKryoRegistrator, KryoSerializer
spark = SparkSession. \
    builder. \
    appName('appName'). \
    config("spark.serializer", KryoSerializer.getName). \
    config("spark.kryo.registrator", SedonaKryoRegistrator.getName). \
    config('spark.jars.packages',
           'org.apache.sedona:sedona-spark-shaded-3.0_2.12:{{ sedona.current_version }},'
           'org.datasyslab:geotools-wrapper:{{ sedona.current_geotools }}'). \
    getOrCreate()
SedonaRegistrator.registerAll(spark)
```

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

You can then play with [Sedona Python Jupyter notebook](../tutorial/jupyter-notebook.md).
