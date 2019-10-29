

# Introduction

Package is a Python wrapper on scala library GeoSparkSQL. Official repository for GeoSpark can be found at https://github.com/DataSystemsLab/GeoSpark.

Package allow to use all GeoSparkSQL functions and transform it to Python Shapely geometry objects. Also it allows to create Spark DataFrame with GeoSpark UDT from Shapely geometry objects. Spark DataFrame can be converted to GeoPandas easily, in addition all fiona drivers for shape file are available to load data from files and convert them to Spark DataFrame. Please look at examples.



# Installation


geo_pyspark depnds on Python packages and Scala libraries. To see all dependencies
please look at Dependencies section.
https://pypi.org/project/pyspark/.

Package needs 3 jar files to work properly:

- geospark-sql_2.2-1.2.0.jar
- geospark-1.2.0.jar
- geo_wrapper.jar

Where 2.2 is a Spark version and 1.2.0 is GeoSpark version. Jar files are placed in geo_pyspark/jars. For newest GeoSpark release jar files are places in subdirectories named as Spark version. Example, jar files for SPARK 2.4 can be found in directory geo_pyspark/jars/2_4.

For older version please find appropriate jar files in directory geo_pyspark/jars/previous.

It is possible to automatically add jar files for newest GeoSpark version. Please use code as follows:


```python

  from pyspark.sql import SparkSession

  from geo_pyspark.register import upload_jars
  from geo_pyspark.register import GeoSparkRegistrator

  upload_jars()

  spark = SparkSession.builder.\
        getOrCreate()

  GeoSparkRegistrator.registerAll(spark)

```

Function

```python

  upload_jars()


```

uses findspark Python package to upload jar files to executor and nodes. To avoid copying all the time, jar files can be put in directory SPARK_HOME/jars or any other path specified in Spark config files.



## Installing from wheel file


```bash

pipenv run python -m pip install dist/geo_pyspark-0.2.0-py3-none-any.whl

```

or

```bash

  pip install dist/geo_pyspark-0.2.0-py3-none-any.whl


```

## Installing from source


```bash

  python3 setup.py install

```

# Core Classes and methods.


`GeoSparkRegistrator.registerAll(spark: pyspark.sql.SparkSession) -> bool`

This is the core of whole package. Class method registers all GeoSparkSQL functions (available for used GeoSparkSQL version).
To check available functions please look at GeoSparkSQL section.
:param spark: pyspark.sql.SparkSession, spark session instance


`upload_jars() -> NoReturn`

Function uses `findspark` Python module to upload newest GeoSpark jars to Spark executor and nodes.

`GeometryType()`

Class which handle serialization and deserialization between GeoSpark geometries and Shapely BaseGeometry types.


