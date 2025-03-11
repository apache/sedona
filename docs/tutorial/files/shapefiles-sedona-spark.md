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

# Shapefiles with Apache Sedona and Spark

This post explains how to read Shapefiles with Apache Sedona and Spark.

A Shapefile is “an Esri vector data storage format for storing the location, shape, and attributes of geographic features.”  The Shapefile format is proprietary, but [the spec is open](https://www.esri.com/content/dam/esrisites/sitecore-archive/Files/Pdfs/library/whitepapers/pdfs/shapefile.pdf).

Shapefiles have many limitations but are extensively used, so it’s beneficial that they are readable by Sedona.

Let’s look at how to read Shapefiles with Sedona and Spark.

## Read Shapefiles with Sedona and Spark

Let’s start by creating a Shapefile with GeoPandas and Shapely:

```python
import geopandas as gpd
from shapely.geometry import Point

point1 = Point(0, 0)
point2 = Point(1, 1)

data = {
    'name': ['Point A', 'Point B'],
    'value': [10, 20],
    'geometry': [point1, point2]
}

gdf = gpd.GeoDataFrame(data, geometry='geometry')
gdf.to_file("/tmp/my_geodata.shp")
```

Here are the files that are output:

```
/tmp/
  my_geodata.cpg
  my_geodata.dbf
  my_geodata.shp
  my_geodata.shx
```

Shapefiles are not stored in a single file.  They contain data in many different files.

Here’s how to read a Shapefile into a Sedona DataFrame powered by Spark:

```python
df = sedona.read.format("shapefile").load("/tmp/my_geodata.shp")
df.show()
```

```
+-----------+-------+-----+
|   geometry|   name|value|
+-----------+-------+-----+
|POINT (0 0)|Point A|   10|
|POINT (1 1)|Point B|   20|
+-----------+-------+-----+
```

You can also see the unique record number for each row in the Shapefile as follows:

```python
df = (
    sedona.read.format("shapefile")
    .option("key.name", "FID")
    .load("/tmp/my_geodata.shp")
)
```

```
+-----------+---+-------+-----+
|   geometry|FID|   name|value|
+-----------+---+-------+-----+
|POINT (0 0)|  1|Point A|   10|
|POINT (1 1)|  2|Point B|   20|
+-----------+---+-------+-----+
```

The name of the geometry column is geometry by default. You can change the name of the geometry column using the `geometry.name` option. Suppose one of the non-spatial attributes is named "geometry", `geometry.name` must be configured to avoid conflict.

```python
df = sedona.read.format("shapefile").option("geometry.name", "geom").load("/path/to/shapefile")
```

The character encoding of string attributes are inferred from the `.cpg` file. If you see garbled values in string fields, you can manually specify the correct charset using the `charset` option. For example:

=== "Scala/Java"

    ```scala
    val df = sedona.read.format("shapefile").option("charset", "UTF-8").load("/path/to/shapefile")
    ```

=== "Java"

    ```java
    Dataset<Row> df = sedona.read().format("shapefile").option("charset", "UTF-8").load("/path/to/shapefile")
    ```

=== "Python"

    ```python
    df = sedona.read.format("shapefile").option("charset", "UTF-8").load("/path/to/shapefile")
    ```

Let’s see how to load many Shapefiles into a Sedona DataFrame.

## Load many Shapefiles with Sedona

Suppose you have a directory with many Shapefiles as follows:

```
/tmp/shapefiles/
  file1.cpg
  file1.dbf
  file1.shp
  file1.shx
  file2.cpg
  file2.dbf
  file2.shp
  file2.shx
```

The directory contains two `.shp` files and other supporting files.

Here’s how to load many Shapefiles into a Sedona DataFrame:

```python
df = sedona.read.format("shapefile").load("/tmp/shapefiles")
df.show()
```

```
+-----------+-------+-----+
|   geometry|   name|value|
+-----------+-------+-----+
|POINT (0 0)|Point A|   10|
|POINT (1 1)|Point B|   20|
|POINT (2 2)|Point C|   10|
|POINT (3 3)|Point D|   20|
+-----------+-------+-----+
```

You can just pass the directory where the Shapefiles are stored, and the Sedona reader will pick them up.

The input path can be a directory containing one or multiple Shapefiles or a path to a `.shp` file.

* All shapefiles directly under the directory will be loaded when the input path is a directory. If you want to load all shapefiles in subdirectories, please specify `.option("recursiveFileLookup", "true")`.
* The shapefile will be loaded when the input path is a .shp file. Sedona will look for sibling files (.dbf, .shx, etc.) with the same main file name and load them automatically.

## Advantages of Shapefiles

Shapefiles are deeply integrated into the Esri ecosystem and extensively used in many services.

You can output a Shapefile from Esri and then read it with another engine like Sedona.

However, Esri created the Shapefile format in the early 1990s, so it has many limitations.

## Limitations of Shapefiles

Here are some of the disadvantages of Shapefiles:

* Don’t support complex geometries
* They don’t support NULL values
* They round numbers
* Bad Unicode support
* Don’t allow for long field names
* 2GB file size limit
* Spatial indexes are slower compared to alternatives
* Unable to store datetimes

See this page for more information on [the limitations of Shapefiles](http://switchfromshapefile.org/).

Due to these limitations, other options are worth investigating.

## Shapefile alternatives

There are a variety of other file formats that are good for geometric data:

* Iceberg
* [GeoParquet](geoparquet-sedona-spark.md)
* FlatGeoBuf
* [GeoPackage](geopackage-sedona-spark.md)
* [GeoJSON](geojson-sedona-spark.md)
* [CSV](csv-geometry-sedona-spark.md)
* GeoTIFF

## Why Sedona does not support Shapefile writes

Sedona does not write Shapefiles for two main reasons:

1. Each Shapefile is a collection of files, which is hard for distributed systems to write.
2. A Shapefile has a hard 2 GB size limit, which isn’t large enough for some spatial data.

## Conclusion

Shapefiles are a legacy file format still used in many production applications. However, they have many limitations and aren’t the best option in a modern data pipeline unless you need compatibility with legacy systems.
