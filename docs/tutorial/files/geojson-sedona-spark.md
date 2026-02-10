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

# Apache Sedona GeoJSON with Spark

This page shows how to read/write single-line GeoJSON files and multiline GeoJSON files with Apache Sedona and Spark.

The post concludes with a summary of the benefits and drawbacks of the GeoJSON file format for spatial analyses.

GeoJSON is based on JSON and supports the following types:

* Point
* LineString
* Polygon
* MultiPoint
* MultiLineString
* MultiPolygon

See here for [more details about the GeoJSON format specification](https://datatracker.ietf.org/doc/html/rfc7946).

## Read multiline GeoJSON files with Sedona and Spark

Here’s how to read a multiline GeoJSON file with Sedona:

```python
df = (
    sedona.read.format("geojson")
    .option("multiLine", "true")
    .load("data/multiline_geojson.json")
    .selectExpr("explode(features) as features")
    .select("features.*")
    .withColumn("prop0", expr("properties['prop0']"))
    .drop("properties")
    .drop("type")
)
df.show(truncate=False)
```

Here’s the output:

```
+---------------------------------------------+------+
|geometry                                     |prop0 |
+---------------------------------------------+------+
|POINT (102 0.5)                              |value0|
|LINESTRING (102 0, 103 1, 104 0, 105 1)      |value1|
|POLYGON ((100 0, 101 0, 101 1, 100 1, 100 0))|value2|
+---------------------------------------------+------+
```

The multiline GeoJSON file contains a point, a linestring, and a polygon.  Let’s inspect the content of the file:

```json
{ "type": "FeatureCollection",
    "features": [
      { "type": "Feature",
        "geometry": {"type": "Point", "coordinates": [102.0, 0.5]},
        "properties": {"prop0": "value0"}
        },
      { "type": "Feature",
        "geometry": {
          "type": "LineString",
          "coordinates": [
            [102.0, 0.0], [103.0, 1.0], [104.0, 0.0], [105.0, 1.0]
            ]
          },
        "properties": {
          "prop0": "value1",
          "prop1": 0.0
          }
        },
      { "type": "Feature",
         "geometry": {
           "type": "Polygon",
           "coordinates": [
             [ [100.0, 0.0], [101.0, 0.0], [101.0, 1.0],
               [100.0, 1.0], [100.0, 0.0] ]
             ]
         },
         "properties": {
           "prop0": "value2",
           "prop1": {"this": "that"}
           }
         }
       ]
}
```

Notice how the data is modeled as a `FeatureCollection`.  Each feature has a geometry type, geometry coordinates, and properties.

You can also read many multiline GeoJSON files.  Suppose you have the following GeoJSON files:

```
many_geojsons/
  file1.json
  file2.json
```

Here's how you can read many GeoJSON files:

```python
df = (
    sedona.read.format("geojson").option("multiLine", "true").load("data/many_geojsons")
)
```

You just need to pass the directory that contains the JSON files.

Multiline GeoJSON is nicely formatted for humans but inefficient for machines. It’s better to store all the JSON data in a single line.

## Read single-line GeoJSON files with Sedona and Spark

Here’s how to read single-line GeoJSON files with Sedona:

```python
df = (
    sedona.read.format("geojson")
    .load("data/singleline_geojson.json")
    .withColumn("prop0", expr("properties['prop0']"))
    .drop("properties")
    .drop("type")
)
df.show(truncate=False)
```

Here’s the result:

```
+---------------------------------------------+------+
|geometry                                     |prop0 |
+---------------------------------------------+------+
|POINT (102 0.5)                              |value0|
|LINESTRING (102 0, 103 1, 104 0, 105 1)      |value1|
|POLYGON ((100 0, 101 0, 101 1, 100 1, 100 0))|value2|
+---------------------------------------------+------+
```

Here’s the data:

```
{"type":"Feature","geometry":{"type":"Point","coordinates":[102.0,0.5]},"properties":{"prop0":"value0"}}
{"type":"Feature","geometry":{"type":"LineString","coordinates":[[102.0,0.0],[103.0,1.0],[104.0,0.0],[105.0,1.0]]},"properties":{"prop0":"value1"}}
{"type":"Feature","geometry":{"type":"Polygon","coordinates":[[[100.0,0.0],[101.0,0.0],[101.0,1.0],[100.0,1.0],[100.0,0.0]]]},"properties":{"prop0":"value2"}}
```

Notice how the multi-line GeoJSON uses a `FeatureCollection` whereas each single-line GeoJSON row uses a different `Feature`.

Single-line GeoJSON files are better because they’re splittable by query engines.

Now, let's see how to create GeoJSON files with Sedona by writing out DataFrames.

## Write to GeoJSON with Sedona and Spark

Let’s create a Sedona DataFrame and then write it out to GeoJSON files:

```
df = sedona.createDataFrame([
    ("a", 'LINESTRING(2.0 5.0,6.0 1.0)'),
    ("b", 'LINESTRING(7.0 4.0,9.0 2.0)'),
    ("c", 'LINESTRING(1.0 3.0,3.0 1.0)'),
], ["id", "geometry"])
actual = df.withColumn("geometry", ST_GeomFromText(col("geometry")))
actual.write.format("geojson").mode("overwrite").save("/tmp/a_thing")
```

Here are the files that get written:

```
a_thing/
  _SUCCESS
  part-00000-856044c5-ae35-4306-bf7a-ae9c3cb25434-c000.json
  part-00003-856044c5-ae35-4306-bf7a-ae9c3cb25434-c000.json
  part-00007-856044c5-ae35-4306-bf7a-ae9c3cb25434-c000.json
  part-00011-856044c5-ae35-4306-bf7a-ae9c3cb25434-c000.json
```

Sedona writes multiple GeoJSON files in parallel, which is faster than writing a single file.

Note that the DataFrame must contain at least one column with geometry type for the write operation to work. Sedona will use the following rules to determine which column to use as the geometry:

1. If there's a column named "geometry" with geometry type, Sedona will use this column
2. Otherwise, Sedona will use the first geometry column found in the root schema

You can also manually specify which geometry column to use with the "geometry.column" option:

```python
df.write.format("geojson").option("geometry.column", "geometry").save("/tmp/a_thing")
```

Now let’s read these GeoJSON files into a DataFrame:

```python
df = sedona.read.format("geojson").load("/tmp/a_thing")
df.show(truncate=False)
```

```
+---------------------+----------+-------+
|geometry             |properties|type   |
+---------------------+----------+-------+
|LINESTRING (1 3, 3 1)|{c}       |Feature|
|LINESTRING (2 5, 6 1)|{a}       |Feature|
|LINESTRING (7 4, 9 2)|{b}       |Feature|
+---------------------+----------+-------+
```

## Benefits of the GeoJSON file format

The GeoJSON file format has many advantages:

* It is human-readable
* It can be output in multiple files, which allows for faster I/O for parallel processing engines.
* Many engines support GeoJSON / JSON files.

However, GeoJSON has many downsides, making it a suboptimal choice for storing geospatial data.

## Limitations of the GeoJSON file format

The GeoJSON format has many limitations that can make it a slow option for spatial data lakes:

* A GeoJSON object may have a CRS, but it's optional, so this critical data can be lost.
* It’s a row-oriented file format, so performance optimizations like column pruning aren’t available (column-oriented file formats, like GeoParquet, can take advantage of this optimization).
* It does not store metadata information on row groups, so row-group filtering isn’t possible (row-group filtering is a Parquet performance optimization).
* The schema is not specified in the footer, so it needs to be manually written or inferred.
* The GeoJSON specification requires a specific structure that can be rigid for certain types of datasets.
* You can only build GeoJSON data lakes.  You can’t use GeoJSON to build data lakehouses.

## Conclusion

GeoJSON is a common file format in spatial data analyses, and it’s convenient that Apache Sedona offers full read and write capabilities.

GeoJSON is well-supported and human-readable, but it’s pretty slow compared to formats like GeoParquet.  It’s generally best to use GeoParquet or Iceberg for spatial data analyses because the performance is much better.
