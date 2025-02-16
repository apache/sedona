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

# Apache Sedona GeoParquet with Spark

This page explains how to build spatial data lakes with Apache Sedona and GeoParquet.

GeoParquet offers many advantages for spatial data sets:

* Built-in support for geometry columns
* GeoParquet's columnar nature allows for column pruning, which speeds up queries that use only a subset of columns.
* Embedded statistics enable readers to skip reading entire chunks of a file in some queries.
* Storing bounding-box metadata (“bbox”) for each row group allows for row-group filtering.
* Columnar file formats are easy to compress.
* You don’t need to infer or manually specify the schema as you would have to with GeoJSON or CSV because GeoParquet includes it in the footer.

Let's see how to write a Sedona DataFrame to GeoParquet files.

## Write Sedona DataFrame to GeoParquet with Spark

Start by creating a Sedona DataFrame:

```python
df = sedona.createDataFrame([
    ("a", 'LINESTRING(2.0 5.0,6.0 1.0)'),
    ("b", 'LINESTRING(7.0 4.0,9.0 2.0)'),
    ("c", 'LINESTRING(1.0 3.0,3.0 1.0)'),
], ["id", "geometry"])
df = df.withColumn("geometry", ST_GeomFromText(col("geometry")))
```

Now write the DataFrame to GeoParquet files:

```python
df.write.format("geoparquet").mode("overwrite").save("/tmp/somewhere")
```

Here are the files created in storage:

```
somewhere/
  _SUCCESS
  part-00000-1c13be9e-6d4c-401e-89d8-739000ad3aba-c000.snappy.parquet
  part-00003-1c13be9e-6d4c-401e-89d8-739000ad3aba-c000.snappy.parquet
  part-00007-1c13be9e-6d4c-401e-89d8-739000ad3aba-c000.snappy.parquet
  part-00011-1c13be9e-6d4c-401e-89d8-739000ad3aba-c000.snappy.parquet
```

Sedona writes many Parquet files in parallel because that's much faster than writing a single file.

## Read GeoParquet files into Sedona DataFrame with Spark

Let's read the GeoParquet files into a Sedona DataFrame:

```python
df = sedona.read.format("geoparquet").load("/tmp/somewhere")
df.show(truncate=False)
```

Here are the results:

```
+---+---------------------+                                                 
|id |geometry             |
+---+---------------------+
|a  |LINESTRING (2 5, 6 1)|
|b  |LINESTRING (7 4, 9 2)|
|c  |LINESTRING (1 3, 3 1)|
+---+---------------------+
```

Here's how Sedona executes this query under the hood:

1. It fetches the schema from the footer of a GeoParquet file, so no schema inference is needed.
2. It collects the bbox metadata for each file and sees if any data can be skipped.
3. It runs the query, skipping any columns that are not required.

Sedona queries on GeoParquet files can often be executed faster and more reliably than other file formats, like CSV.  CSV files don't store the schema in the file footer, don't support column pruning, and don't have row-group metadata for data skipping.

## Read GeoParquet metadata

Here’s how to read the extra metadata that GeoParquet adds to the footer of the Parquet file.

```python
df = sedona.read.format("geoparquet.metadata").load("/tmp/somewhere")
```

Let’s check out the content of the metadata and the schema:

```
df.show(truncate=False)

+-----------------------------+-------------------------------------------------------------------+
|path                         |columns                                                            |
+-----------------------------+-------------------------------------------------------------------+
|file:/part-00003-1c13.parquet|{geometry -> {WKB, [LineString], [2.0, 1.0, 6.0, 5.0], null, NULL}}|
|file:/part-00007-1c13.parquet|{geometry -> {WKB, [LineString], [7.0, 2.0, 9.0, 4.0], null, NULL}}|
|file:/part-00011-1c13.parquet|{geometry -> {WKB, [LineString], [1.0, 1.0, 3.0, 3.0], null, NULL}}|
|file:/part-00000-1c13.parquet|{geometry -> {WKB, [], [0.0, 0.0, 0.0, 0.0], null, NULL}}          |
+-----------------------------+-------------------------------------------------------------------+
```

The `columns` column contains bounding box information on each file in the GeoParquet data lake.  These bbox values are useful for skipping files or row-groups.  More will come on bbox metadata in the following section.

Let’s check out the schema of the metadata:

```
df.printSchema()

root
 |-- path: string (nullable = true)
 |-- version: string (nullable = true)
 |-- primary_column: string (nullable = true)
 |-- columns: map (nullable = true)
 |    |-- key: string
 |    |-- value: struct (valueContainsNull = true)
 |    |    |-- encoding: string (nullable = true)
 |    |    |-- geometry_types: array (nullable = true)
 |    |    |    |-- element: string (containsNull = true)
 |    |    |-- bbox: array (nullable = true)
 |    |    |    |-- element: double (containsNull = true)
 |    |    |-- crs: string (nullable = true)
 |    |    |-- covering: string (nullable = true)
```

The GeoParquet footer stores the following data for each column:

* geometry_type: type of geometric object like point or polygon
* bbox: bounding box of objects in file
* crs: Coordinate Reference System
* covering: a struct column of xmin, ymin, xmax, ymax that provides the row group statistics for GeoParquet 1.1 files

The rest of the Parquet metadata is stored in the Parquet footer, right where it is stored for other Parquet files too.

Let's look closer at how Sedona uses the GeoParquet bbox metadata to optimize queries.

## How Sedona uses GeoParquet bounding box (bbox) metadata with Spark

The bounding box metadata specifies the area covered by geometric shapes in a given file.  Suppose you query points in a region not covered by the bounding box for a given file.  The engine can skip that entire file when executing the query because it’s known that it does not cover any relevant data.

Skipping entire files of data can massively improve performance.  The more data that’s skipped, the faster the query will run.

Let’s look at an example of a dataset with points and three bounding boxes.

![GeoParquet bbox](../../image/tutorial/files/geoparquet_bbox1.png)

Now, let’s apply a spatial filter to read points within a particular area:

![GeoParquet bbox filter](../../image/tutorial/files/geoparquet_bbox1.png)

Here is the query:

```python
my_shape = 'POLYGON((4.0 3.5, 4.0 6.0, 8.0 6.0, 8.0 4.5, 4.0 3.5))'

res = sedona.sql(f'''
select *
from points
where st_intersects(geometry, ST_GeomFromWKT('{my_shape}'))
''')
res.show(truncate=False)
```

Here are the results:

```
+---+-----------+
|id |geometry   |
+---+-----------+
|e  |POINT (5 4)|
|f  |POINT (6 4)|
|g  |POINT (6 5)|
+---+-----------+
```

We don’t need the data in bounding boxes 1 or 3 to run this query.  We just need points in bounding box 2.  File skipping lets us read one file instead of three for this query.  Here are the bounding boxes for the files in the GeoParquet data lake:

```
+--------------------------------------------------------------+
|columns                                                       |
+--------------------------------------------------------------+
|{geometry -> {WKB, [Point], [2.0, 1.0, 3.0, 3.0], null, NULL}}|
|{geometry -> {WKB, [Point], [7.0, 1.0, 8.0, 2.0], null, NULL}}|
|{geometry -> {WKB, [Point], [5.0, 4.0, 6.0, 5.0], null, NULL}}|
+--------------------------------------------------------------+
```

Skipping entire files can result in massive performance gains.

## Advantages of GeoParquet files

As previously mentioned, GeoParquet files have many advantages over row-oriented files like CSV or GeoJSON:

* Column pruning
* Row group filtering
* Schema in the footer
* Good compressibility because of the columnar data layout

These are huge advantages but don’t offer the complete feature set that spatial data practitioners need.

## Limitations of GeoParquet files

Parquet data lakes and GeoParquet data lakes face many of the same limitations as other data lakes:

* They don’t support reliable transactions
* Some Data Manipulation Language (“DML”) operations (e.g., update, delete) are not supported
* No concurrency protection
* Poor performance compared to databases for certain operations

To overcome these limitations, the data community has migrated to open table formats like Iceberg, Delta Lake, and Hudi.

Iceberg recently rolled out support for geometry and geography columns.  This allows data practitioners to get the best features of open table formats and the benefits of Parquet files, where the data is stored.  Iceberg overcomes the limitations of GeoParquet data lakes and allows for reliable transactions and easy access to common operations like updates and deletes.

## Conclusion

GeoParquet is a significant innovation for the spatial data community.  It provides geo engineers with embedded schemas, performance optimizations, and native support for geometry columns.

Spatial data engineers can also migrate to Iceberg now that it supports all these positive features of GeoParquet, and even more.  Iceberg provides many useful open table features and is almost always a better option than vanilla GeoParquet (except for single file datasets that will never change or for compatibility with other engines).

Switching from legacy file formats like Shapefile, GeoPackage, CSV, GeoJSON to high-performance file formats like GeoParquet/Iceberg should immediately speed up your computational workflows.
