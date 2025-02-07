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

# Work with GeoPandas and Shapely

!!! note
	Sedona before 1.6.0 only works with Shapely 1.x. If you want to work with Shapely 2.x, please use Sedona no earlier than 1.6.0.

    If you use Sedona < 1.6.0, please use GeoPandas <= `0.11.1` since GeoPandas > 0.11.1 will automatically install Shapely 2.0. If you use Shapely, please use <= `1.8.5`.

## Interoperate with GeoPandas

Sedona Python has implemented serializers and deserializers which allows to convert Sedona Geometry objects into Shapely BaseGeometry objects. Based on that it is possible to load the data with geopandas from file (look at Fiona possible drivers) and create Spark DataFrame based on GeoDataFrame object.

### From GeoPandas to Sedona DataFrame

Loading the data from shapefile using geopandas read_file method and create Spark DataFrame based on GeoDataFrame:

```python

import geopandas as gpd
from sedona.spark import *

config = SedonaContext.builder().\
      getOrCreate()

sedona = SedonaContext.create(config)

gdf = gpd.read_file("gis_osm_pois_free_1.shp")

sedona.createDataFrame(
  gdf
).show()

```

This query will show the following outputs:

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

```

### From Sedona DataFrame to GeoPandas

Reading data with Spark and converting to GeoPandas

```python

import geopandas as gpd
from sedona.spark import *

config = SedonaContext.builder().
	getOrCreate()

sedona = SedonaContext.create(config)

counties = sedona.\
    read.\
    option("delimiter", "|").\
    option("header", "true").\
    csv("counties.csv")

counties.createOrReplaceTempView("county")

counties_geom = sedona.sql(
    "SELECT *, st_geomFromWKT(geom) as geometry from county"
)

df = counties_geom.toPandas()
gdf = gpd.GeoDataFrame(df, geometry="geometry")

gdf.plot(
    figsize=(10, 8),
    column="value",
    legend=True,
    cmap='YlOrBr',
    scheme='quantiles',
    edgecolor='lightgray'
)

```

<br>
<br>

![poland_image](https://user-images.githubusercontent.com/22958216/67603296-c08b4680-f778-11e9-8cde-d2e14ffbba3b.png)

<br>
<br>

You may also wish to try converting to GeoPandas via GeoArrow, which can be
significantly faster for large results (requires geopandas >= 1.0).

```python
import geopandas as gpd
from sedona.spark import dataframe_to_arrow

config = SedonaContext.builder().
	getOrCreate()

sedona = SedonaContext.create(config)

test_wkt = ["POINT (0 1)", "LINESTRING (0 1, 2 3)"]
df = sedona.createDataFrame(zip(test_wkt), ["wkt"]).selectExpr(
    "ST_GeomFromText(wkt) as geom"
)

gpd.GeoDataFrame.from_arrow(dataframe_to_arrow(df))
```

## Interoperate with shapely objects

### Supported Shapely objects

| shapely object     | Available          |
|--------------------|--------------------|
| Point              | :heavy_check_mark: |
| MultiPoint         | :heavy_check_mark: |
| LineString         | :heavy_check_mark: |
| MultiLinestring    | :heavy_check_mark: |
| Polygon            | :heavy_check_mark: |
| MultiPolygon       | :heavy_check_mark: |
| GeometryCollection | :heavy_check_mark: |

To create Spark DataFrame based on mentioned Geometry types, please use <b> GeometryType </b> from  <b> sedona.sql.types </b> module. Converting works for list or tuple with shapely objects.

Schema for target table with integer id and geometry type can be defined as follows:

```python

from pyspark.sql.types import IntegerType, StructField, StructType

from sedona.spark import *

schema = StructType(
    [
        StructField("id", IntegerType(), False),
        StructField("geom", GeometryType(), False)
    ]
)

```

Also, Spark DataFrame with geometry type can be converted to list of shapely objects with <b> collect </b> method.

### Point example

```python
from shapely.geometry import Point

data = [
    [1, Point(21.0, 52.0)],
    [1, Point(23.0, 42.0)],
    [1, Point(26.0, 32.0)]
]


gdf = sedona.createDataFrame(
    data,
    schema
)

gdf.show()

```

```
+---+-------------+
| id|         geom|
+---+-------------+
|  1|POINT (21 52)|
|  1|POINT (23 42)|
|  1|POINT (26 32)|
+---+-------------+
```

```python
gdf.printSchema()
```

```
root
 |-- id: integer (nullable = false)
 |-- geom: geometry (nullable = false)
```

### MultiPoint example

```python3

from shapely.geometry import MultiPoint

data = [
    [1, MultiPoint([[19.511463, 51.765158], [19.446408, 51.779752]])]
]

gdf = sedona.createDataFrame(
    data,
    schema
).show(1, False)

```

```

+---+---------------------------------------------------------+
|id |geom                                                     |
+---+---------------------------------------------------------+
|1  |MULTIPOINT ((19.511463 51.765158), (19.446408 51.779752))|
+---+---------------------------------------------------------+


```

### LineString example

```python3

from shapely.geometry import LineString

line = [(40, 40), (30, 30), (40, 20), (30, 10)]

data = [
    [1, LineString(line)]
]

gdf = sedona.createDataFrame(
    data,
    schema
)

gdf.show(1, False)

```

```

+---+--------------------------------+
|id |geom                            |
+---+--------------------------------+
|1  |LINESTRING (10 10, 20 20, 10 40)|
+---+--------------------------------+

```

### MultiLineString example

```python3

from shapely.geometry import MultiLineString

line1 = [(10, 10), (20, 20), (10, 40)]
line2 = [(40, 40), (30, 30), (40, 20), (30, 10)]

data = [
    [1, MultiLineString([line1, line2])]
]

gdf = sedona.createDataFrame(
    data,
    schema
)

gdf.show(1, False)

```

```

+---+---------------------------------------------------------------------+
|id |geom                                                                 |
+---+---------------------------------------------------------------------+
|1  |MULTILINESTRING ((10 10, 20 20, 10 40), (40 40, 30 30, 40 20, 30 10))|
+---+---------------------------------------------------------------------+

```

### Polygon example

```python3

from shapely.geometry import Polygon

polygon = Polygon(
    [
         [19.51121, 51.76426],
         [19.51056, 51.76583],
         [19.51216, 51.76599],
         [19.51280, 51.76448],
         [19.51121, 51.76426]
    ]
)

data = [
    [1, polygon]
]

gdf = sedona.createDataFrame(
    data,
    schema
)

gdf.show(1, False)

```

```

+---+--------------------------------------------------------------------------------------------------------+
|id |geom                                                                                                    |
+---+--------------------------------------------------------------------------------------------------------+
|1  |POLYGON ((19.51121 51.76426, 19.51056 51.76583, 19.51216 51.76599, 19.5128 51.76448, 19.51121 51.76426))|
+---+--------------------------------------------------------------------------------------------------------+

```

### MultiPolygon example

```python3

from shapely.geometry import MultiPolygon

exterior_p1 = [(0, 0), (0, 2), (2, 2), (2, 0), (0, 0)]
interior_p1 = [(1, 1), (1, 1.5), (1.5, 1.5), (1.5, 1), (1, 1)]

exterior_p2 = [(0, 0), (1, 0), (1, 1), (0, 1), (0, 0)]

polygons = [
    Polygon(exterior_p1, [interior_p1]),
    Polygon(exterior_p2)
]

data = [
    [1, MultiPolygon(polygons)]
]

gdf = sedona.createDataFrame(
    data,
    schema
)

gdf.show(1, False)

```

```

+---+----------------------------------------------------------------------------------------------------------+
|id |geom                                                                                                      |
+---+----------------------------------------------------------------------------------------------------------+
|1  |MULTIPOLYGON (((0 0, 0 2, 2 2, 2 0, 0 0), (1 1, 1.5 1, 1.5 1.5, 1 1.5, 1 1)), ((0 0, 0 1, 1 1, 1 0, 0 0)))|
+---+----------------------------------------------------------------------------------------------------------+

```

### GeometryCollection example

```python3

from shapely.geometry import GeometryCollection, Point, LineString, Polygon

exterior_p1 = [(0, 0), (0, 2), (2, 2), (2, 0), (0, 0)]
interior_p1 = [(1, 1), (1, 1.5), (1.5, 1.5), (1.5, 1), (1, 1)]
exterior_p2 = [(0, 0), (1, 0), (1, 1), (0, 1), (0, 0)]

geoms = [
    Polygon(exterior_p1, [interior_p1]),
    Polygon(exterior_p2),
    Point(1, 1),
    LineString([(0, 0), (1, 1), (2, 2)])
]

data = [
    [1, GeometryCollection(geoms)]
]

gdf = sedona.createDataFrame(
    data,
    schema
)

gdf.show(1, False)
```

```
+---+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|id |geom                                                                                                                                                                     |
+---+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|1  |GEOMETRYCOLLECTION (POLYGON ((0 0, 0 2, 2 2, 2 0, 0 0), (1 1, 1 1.5, 1.5 1.5, 1.5 1, 1 1)), POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0)), POINT (1 1), LINESTRING (0 0, 1 1, 2 2))|
+---+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------+

```
