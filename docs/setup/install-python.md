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

The base `apache-sedona` package installs `shapely` and `attrs`. Using Sedona with Spark also requires PySpark, which is often preinstalled on managed Spark platforms.

Use `pip` to install the published package and any extras you need. Sedona uses [uv](https://docs.astral.sh/uv/) to manage dependencies when developing the project. Package dependencies and version constraints are defined in [python/pyproject.toml](https://github.com/apache/sedona/blob/master/python/pyproject.toml).

### Install sedona

* Installing from PyPI repositories. You can find the latest Sedona Python on [PyPI](https://pypi.org/project/apache-sedona/). [There is a known issue in Sedona v1.0.1 and earlier versions](release-notes.md#known-issue).

```bash
pip install apache-sedona
```

* Since Sedona v1.1.0, pyspark is an optional dependency of Sedona Python because spark comes pre-installed on many spark platforms. To install pyspark along with Sedona Python in one go, use the `spark` extra:

```bash
pip install "apache-sedona[spark]"
```

* Installing from Sedona Python source

Clone Sedona GitHub source code and run the following command

```bash
cd python
python3 -m pip install .
```

### Optional dependencies

An extra installs additional packages for a particular use case. Choose the extras you need:

| Package or extra | Additional packages | Use case |
| :--- | :--- | :--- |
| `apache-sedona` | `shapely`, `attrs` | Base package; use with an existing Spark installation. |
| `spark` | `pyspark` | Install PySpark when it is not already available. |
| `pydeck-map` | `geopandas`, `pydeck` | Create maps with `SedonaPyDeck`. |
| `kepler-map` | `geopandas`, `keplergl` | Create maps with `SedonaKepler`. |
| `flink` | `apache-flink` | Use Sedona with PyFlink. |
| `db` | `sedonadb[geopandas]` | Install SedonaDB with GeoPandas support; requested only on Python 3.9 or later. |
| `all` | `pyspark`, `geopandas`, `pydeck`, `keplergl`, `rasterio` | Install Spark, mapping, and Python raster dependencies together. |

For example, to install the `all` extra:

```bash
pip install "apache-sedona[all]"
```

Despite its name, `all` does not include the `flink` or `db` extra. Extras also do not install the Sedona JVM jars described below.

The `rasterio` package supports Python-side raster objects. SQL readers for existing raster files do not require it.

On a managed Spark platform such as EMR, keep the platform's PySpark installation. To add both mapping libraries without requesting PySpark, combine the mapping extras:

```bash
pip install "apache-sedona[pydeck-map,kepler-map]"
```

GeoPandas brings in `pandas` as a dependency. PyArrow is also needed for Arrow-based conversions and pandas-on-Spark APIs; the `spark` and `all` extras do not explicitly request it. Install versions of `pandas` and `pyarrow` compatible with your Spark version when using these APIs. See [working with GeoPandas and Shapely](../tutorial/geopandas-shapely.md) for conversion examples.

### Prepare sedona-spark jar

Sedona Python needs one additional jar file called `sedona-spark-shaded` or `sedona-spark` to work properly. Please make sure you use the correct version for Spark and Scala.

Please use Spark major.minor version number in artifact names.

You can get it using one of the following methods:

1. If you run Sedona in Databricks, AWS EMR, or other cloud platform's notebook, use the `shaded jar`: Download [sedona-spark-shaded jar](https://repo.maven.apache.org/maven2/org/apache/sedona/) and [geotools-wrapper jar](https://repo.maven.apache.org/maven2/org/datasyslab/geotools-wrapper/) from Maven Central, and put them in SPARK_HOME/jars/ folder.
2. If you run Sedona in an IDE or a local Jupyter notebook, use the `unshaded jar`. Call the [Maven Central coordinate](maven-coordinates.md) in your python program. For example,
==Sedona >= 1.4.1==

```python
from sedona.spark import *

config = (
    SedonaContext.builder()
    .config(
        "spark.jars.packages",
        "org.apache.sedona:sedona-spark-3.5_2.12:{{ sedona.current_version }},"
        "org.datasyslab:geotools-wrapper:{{ sedona.current_geotools }}",
    )
    .config(
        "spark.jars.repositories",
        "https://artifacts.unidata.ucar.edu/repository/unidata-all",
    )
    .getOrCreate()
)
sedona = SedonaContext.create(config)
```

==Sedona < 1.4.1==

SedonaRegistrator is deprecated in Sedona 1.4.1 and later versions. Please use the above method instead.

```python
from pyspark.sql import SparkSession
from sedona.spark import SedonaRegistrator
from sedona.spark import SedonaKryoRegistrator, KryoSerializer

spark = (
    SparkSession.builder.appName("appName")
    .config("spark.serializer", KryoSerializer.getName)
    .config("spark.kryo.registrator", SedonaKryoRegistrator.getName)
    .config(
        "spark.jars.packages",
        "org.apache.sedona:sedona-spark-shaded-3.5_2.12:{{ sedona.current_version }},"
        "org.datasyslab:geotools-wrapper:{{ sedona.current_geotools }}",
    )
    .getOrCreate()
)
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

### Spatial values on Spark 4.2 and later

Spark 4.2 requires Python 3.10 or later. Sedona uses Spark's native `GeometryType` and `GeographyType` on Spark 4.2+. On older Spark versions, Sedona continues to use its existing UDTs and Shapely geometry values.

On Spark 4.2+, `collect()` returns `pyspark.sql.types.Geometry` or `Geography` values containing WKB and an SRID. Convert explicitly at the Python boundary:

```python
import shapely
from shapely.geometry import Point
from pyspark.sql.types import StructField, StructType
from sedona.spark import GeometryType, to_spark_geometry, to_shapely

point = shapely.set_srid(Point(1, 2), 4326)
schema = StructType([StructField("geom", GeometryType(4326), nullable=True)])
df = sedona.createDataFrame([(to_spark_geometry(point),), (None,)], schema)
point_again = to_shapely(df.first().geom)
assert shapely.get_srid(point_again) == 4326
```

The conversion helpers require Shapely 2 or later for native spatial values. Shapely 2.1+ is required to preserve measured (M/ZM) geometries. They preserve nulls and embedded SRIDs. `to_spark_geography` accepts a Shapely geometry or Sedona `Geography` wrapper; an unset Shapely SRID (0) defaults to 4326 on Spark 4.2+. `to_shapely` accepts either native spatial value. Sedona's distributed GeoPandas constructors and `to_geopandas()` handle these conversions internally. Spatial visualization helpers require GeoPandas 1.0 or later on Spark 4.2+.

Unlike the old UDT constructor, the native `GeometryType` and `GeographyType` constructors require an SRID, for example `GeometryType(4326)`, or `"ANY"` for a column whose rows may have different SRIDs. Sedona's internal default schemas use `"ANY"`. Choose a fixed SRID when the column has one known CRS, and check the target file format's support for mixed-SRID types before persisting an `"ANY"` column. Setting an SRID labels coordinates; it does not reproject them.

`ST_AsBinary`, `ST_GeomFromWKB`, `ST_GeogFromWKB`, `ST_SRID`, and `ST_SetSRID` use Spark's native implementations on Spark 4.2+, including through Sedona's Python wrappers. Native `ST_GeogFromWKB` defaults to SRID 4326; the Python wrapper's optional SRID argument applies native `ST_SetSRID` to the parsed geography. Native Spark validates geographic SRIDs, so explicitly requesting SRID 0 is an error. Older Spark keeps Sedona's existing behavior.

`sedona_vectorized_udf` uses Spark's standard pandas UDF protocol for native spatial types on Spark 4.2+. Annotated Shapely scalar and GeoSeries callbacks keep receiving Shapely values; older Spark continues to use Sedona's existing serializer.
