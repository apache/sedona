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

# Apache Sedona GeoPackage with Spark

This page shows how to read GeoPackage files with Apache Sedona and Spark.

You’ll learn about the advantages and disadvantages of the GeoPackage file format and how to use them in production settings.

Let’s start by creating a GeoPackage file and then reading it.

## Reading a GeoPackage file with Sedona and Spark

Let’s create a GeoPackage file with a few rows of data.

Start by creating a GeoPandas DataFrame:

```python
point1 = Point(0, 0)
point2 = Point(1, 1)
polygon1 = Polygon([(5, 5), (6, 6), (7, 5), (6, 4)])

data = {
    "name": ["Point A", "Point B", "Polygon A"],
    "value": [10, 20, 30],
    "geometry": [point1, point2, polygon1],
}
gdf = gpd.GeoDataFrame(data, geometry="geometry")
```

Now write the GeoPandas DataFrame to a GeoPackage file:

```python
gdf.to_file("/tmp/my_file.gpkg", layer="my_layer", driver="GPKG")
```

GeoPandas knows to write this to a GeoPackage file because the code sets the driver to `GPKG`.

You can think of the layer as the table name.

Now let’s read the GeoPackage file Apache Sedona and Spark:

```python
df = (
    sedona.read.format("geopackage")
    .option("tableName", "my_layer")
    .load("/tmp/my_file.gpkg")
)
df.show()
```

Here are the contents of the DataFrame:

```
+---+--------------------+---------+-----+
|fid|                geom|     name|value|
+---+--------------------+---------+-----+
|  1|         POINT (0 0)|  Point A|   10|
|  2|         POINT (1 1)|  Point B|   20|
|  3|POLYGON ((5 5, 6 ...|Polygon A|   30|
+---+--------------------+---------+-----+
```

The geometry column can contain many different geometric objects like points, polygons, and many more.

You can also see the metadata of the GeoPackage file:

```python
df = (
    sedona.read.format("geopackage")
    .option("showMetadata", "true")
    .load("/tmp/my_file.gpkg")
)
df.show()
```

Here are the contents:

```
+----------+---------+----------+-----------+--------------------+-----+-----+-----+-----+------+
|table_name|data_type|identifier|description|         last_change|min_x|min_y|max_x|max_y|srs_id|
+----------+---------+----------+-----------+--------------------+-----+-----+-----+-----+------+
|  my_layer| features|  my_layer|           |2025-02-25 06:28:...|  0.0|  0.0|  7.0|  6.0| 99999|
+----------+---------+----------+-----------+--------------------+-----+-----+-----+-----+------+
```

## Reading many GeoPackage files with Sedona and Spark

You can also read many GeoPackage files with Sedona.  Suppose you have the following GeoPackage files:

```
gpkgs/
  my_file1.gpkg
  my_file2.gpkg
```

Here’s how you can read all the files:

```python
df = (
    sedona.read.format("geopackage")
    .option("tableName", "my_layer")
    .load("/tmp/gpkgs")
)
df.show()
```

Here are the results:

```
+---+--------------------+---------+-----+
|fid|                geom|     name|value|
+---+--------------------+---------+-----+
|  1|         POINT (5 5)|  Point C|   30|
|  2|POLYGON ((5 5, 6 ...|Polygon A|   40|
|  1|         POINT (0 0)|  Point A|   10|
|  2|         POINT (1 1)|  Point B|   20|
+---+--------------------+---------+-----+
```

You just need to supply the directory containing the GeoPackage files, and Sedona can read all of them into a DataFrame.

Sedona is an excellent option for analyzing many GeoPackage files because it can read and process them in parallel.

## Load raster data stored in GeoPackage files

You can also load data from raster tables in the GeoPackage file. To load raster data, you can use the following code.

```python
df = sedona.read.format("geopackage").option("tableName", "raster_table").load("/path/to/geopackage")
```

Here are the contents of the DataFrame:

```
+---+----------+-----------+--------+--------------------+
| id|zoom_level|tile_column|tile_row|           tile_data|
+---+----------+-----------+--------+--------------------+
|  1|        11|        428|     778|GridCoverage2D["c...|
|  2|        11|        429|     778|GridCoverage2D["c...|
|  3|        11|        428|     779|GridCoverage2D["c...|
|  4|        11|        429|     779|GridCoverage2D["c...|
|  5|        11|        427|     777|GridCoverage2D["c...|
+---+----------+-----------+--------+--------------------+
```

Known limitations (v1.7.0):

* webp rasters are not supported
* ewkb geometries are not supported
* filtering based on geometries envelopes are not supported

All points above should be resolved soon; stay tuned!

## Advantages of the GeoPackage file format

The GeoPackage file format has many advantages:

* Any engine can support GeoPackage because it’s an open format.
* It’s mutable, unlike many other formats.
* It saves CRS information, unlike some other formats.
* It can store spatial and raster data.
* It can be read by many engines like GeoPandas, Sedona, and SQLite, of course.

However, the GeoPackage format also has many downsides.

## Disadvantages of GeoPackage

The GeoPackage file format has the following disadvantages:

* It’s row-oriented, so it can’t take advantage of column pruning like columnar file formats.
* It does not support multi-engine concurrency transactions.
* SQLite transactions are supported, but building reliable transactions with other engines would be hard.
* All engines do not fully support it.

## Conclusion

GeoPackage is a solid file format if you’re using SQLite.

It’s excellent that Sedona can read GeoPackage files created by SQLite analyses. This allows you to read GeoPackage files in parallel and analyze massive datasets. You can also run Sedona on a cluster.

If you don’t already use GeoPackage, you should probably use file formats like GeoParquet or Iceberg.
