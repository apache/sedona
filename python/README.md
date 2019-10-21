# geo_pyspark

GeoSpark python bindings.
Documentation in sphinx will be ready soon.

## Introduction

Package is a Python wrapper on scala library <b>GeoSparkSQL</b>. Official repository for GeoSpark can be found at
https://github.com/DataSystemsLab/GeoSpark. 

Package allow to use all GeoSparkSQL functions and transform it to Python Shapely geometry objects. Also
it allows to create Spark DataFrame with GeoSpark UDT from Shapely geometry objects. Spark DataFrame can
be converted to GeoPandas easily, in addition all fiona drivers for shape file are available to load
data from files and convert them to Spark DataFrame. Please look at examples.

## Instalation

Package assumes that Spark is already installed. 
### pipenv

clone repository

```bash
git clone https://github.com/Imbruced/geo_pyspark.git
```
Go into directory and run to install all dependencies if you want to create env from
scratch
```python
pipenv install 
pipenv shell
```

Install package from wheel file
```python
pipenv run python -m pip install dist/geo_pyspark-0.1.0-py3-none-any.whl
```
Or using setup.py file
```python
pipenv run python setup.py install
```

### pip

clone repository

```bash
git clone https://github.com/Imbruced/geo_pyspark.git
```

And install package from wheel file

```python
pip install dist/geo_pyspark-0.1.0-py3-none-any.whl
```

## Example usage

It is possible to add automatically jar files. 
Use the following code

```python
from pyspark.sql import SparkSession

from geo_pyspark.register import upload_jars
from geo_pyspark.register import GeoSparkRegistrator

upload_jars()

spark = SparkSession.builder.\
        getOrCreate()

GeoSparkRegistrator.registerAll(spark)

```

This code will add GeoSpark jars to spark driver and executor.

By default package will not upload jars.


### Basic Usage

```python
from pyspark.sql import SparkSession
from geo_pyspark.register import GeoSparkRegistrator


spark = SparkSession.builder.\
        getOrCreate()

GeoSparkRegistrator.registerAll(spark)

df = spark.sql("""SELECT st_GeomFromWKT('POINT(6.0 52.0)') as geom""")

df.show()

```
    +------------+
    |        geom|
    +------------+
    |POINT (6 52)|
    +------------+

### Converting GeoPandas to Spark DataFrame with GeoSpark Geometry UDT.

```python
import os

import geopandas as gpd
from pyspark.sql import SparkSession

from geo_pyspark.data import data_path
from geo_pyspark.register import GeoSparkRegistrator

spark = SparkSession.builder.\
        getOrCreate()

GeoSparkRegistrator.registerAll(spark)

gdf = gpd.read_file(os.path.join(data_path, "gis_osm_pois_free_1.shp"))

spark.createDataFrame(
    gdf
).show()

```

    +---------+----+-----------+--------------------+--------------------+
    |   osm_id|code|     fclass|                name|            geometry|
    +---------+----+-----------+--------------------+--------------------+
    | 26860257|2422|  camp_site|            de Kroon|POINT (15.3393145...|
    | 26860294|2406|     chalet|      Leśne Ustronie|POINT (14.8709625...|
    | 29947493|2402|      motel|                null|POINT (15.0946636...|
    | 29947498|2602|        atm|                null|POINT (15.0732014...|
    | 29947499|2401|      hotel|                null|POINT (15.0696777...|
    | 29947505|2401|      hotel|                null|POINT (15.0155749...|
    +---------+----+-----------+--------------------+--------------------+
    
    
### Converting Spark DataFrame with GeoSpark Geometry UDT to Geopandas.


```python
import os

import geopandas as gpd
from pyspark.sql import SparkSession

from geo_pyspark.data import data_path
from geo_pyspark.register import GeoSparkRegistrator

spark = SparkSession.builder.\
        getOrCreate()

GeoSparkRegistrator.registerAll(spark)

counties = spark.\
    read.\
    option("delimiter", "|").\
    option("header", "true").\
    csv(os.path.join(data_path, "counties.csv"))
    
counties.createOrReplaceTempView("county")

counties_geom = spark.sql(
        "SELECT *, st_geomFromWKT(geom) as geometry from county"
)

df = counties_geom.toPandas()
gdf = gpd.GeoDataFrame(df, geometry="geometry")
gdf.plot()

```
<img src="https://github.com/Imbruced/geo_pyspark/blob/master/geo_pyspark/data/geopandas_plot.PNG" width="250">