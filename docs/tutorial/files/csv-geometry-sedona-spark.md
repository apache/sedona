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

# Apache Sedona CSV with geometry using Spark

This post shows how to read and write CSV files with geometry columns using Sedona and Spark.

You will learn about the advantages and disadvantages of the CSV file format for storing geometric data.

Let’s start by seeing how to write CSV files with geometric data.

## Write CSV with geometry using Sedona and Spark

Let’s start by creating a DataFrame with Sedona and Spark:

```python
df = sedona.createDataFrame(
    [
        ("a", "LINESTRING(2.0 5.0,6.0 1.0)"),
        ("b", "POINT(1.0 2.0)"),
        ("c", "POLYGON((7.0 1.0,7.0 3.0,9.0 3.0,7.0 1.0))"),
    ],
    ["id", "geometry"],
)
df = df.withColumn("geometry", ST_GeomFromText(col("geometry")))
```

Here are the contents of the DataFrame:

```
+---+------------------------------+
|id |geometry                      |
+---+------------------------------+
|a  |LINESTRING (2 5, 6 1)         |
|b  |POINT (1 2)                   |
|c  |POLYGON ((7 1, 7 3, 9 3, 7 1))|
+---+------------------------------+
```

Now write the DataFrame to a CSV file:

```python
df = df.withColumn("geom_wkt", ST_AsText(col("geometry"))).drop("geometry")
df.repartition(1).write.option("header", True).format("csv").mode("overwrite").save(
    "/tmp/my_csvs"
)
```

Notice that we’re using `repartition(1)` to output the DataFrame as a single file.  It’s usually better to output many files in parallel, making the write operation faster.  We’re just writing to a single file for the simplicity of this example.

Here are the contents of the CSV file:

```
id,geom_wkt
a,"LINESTRING (2 5, 6 1)"
b,POINT (1 2)
c,"POLYGON ((7 1, 7 3, 9 3, 7 1))"
```

This file stores the `geom_wkt` column as plain text, making it easily human-readable.  It follows a standard format, so any engine that knows how to parse WKT can read the column.

## Read CSV with geometry using Sedona and Spark

Now read the CSV file into a DataFrame:

```python
df = (
    sedona.read.option("header", True)
    .format("CSV")
    .load("/tmp/my_csvs")
    .withColumn("geometry", ST_GeomFromText(col("geom_wkt")))
    .drop("geom_wkt")
)
```

This file stores the `geom_wkt` column as text.  When you read the data, you must convert it to a geometry column with the `ST_GeomFromText` function.  Here are the contents of the DataFrame:

```
+---+------------------------------+
|id |geometry                      |
+---+------------------------------+
|a  |LINESTRING (2 5, 6 1)         |
|b  |POINT (1 2)                   |
|c  |POLYGON ((7 1, 7 3, 9 3, 7 1))|
+---+------------------------------+
```

Verify that the schema is correct:

```
root
 |-- id: string (nullable = true)
 |-- geometry: geometry (nullable = true)
```

## Read/write CSV files with Extended Well-Known Text (EWKT)

Let’s see how to write the DataFrame to CSV with EWKT.  Start by adding the SRID to the geometry column.

```python
df = df.withColumn("geometry", ST_SetSRID(col("geometry"), 4326))
```

Now write out the DataFrame with an EWKT column:

```python
df = df.withColumn("geom_ewkt", ST_AsEWKT(col("geometry"))).drop("geometry")
df.repartition(1).write.option("header", True).format("csv").mode("overwrite").save(
    "/tmp/my_ewkt_csvs"
)
```

Here are the contents of the CSV file:

```
id,geom_ewkt
a,"SRID=4326;LINESTRING (2 5, 6 1)"
b,SRID=4326;POINT (1 2)
c,"SRID=4326;POLYGON ((7 1, 7 3, 9 3, 7 1))"
```

Here’s how to read the CSV file with an EWKT column into a Sedona DataFrame:

```python
df = (
    sedona.read.option("header", True)
    .format("csv")
    .load("/tmp/my_ewkt_csvs")
    .withColumn("geometry", ST_GeomFromEWKT(col("geom_ewkt")))
    .drop("geom_ewkt")
)
```

Here are the contents of the DataFrame:

```
+---+------------------------------+
|id |geometry                      |
+---+------------------------------+
|a  |LINESTRING (2 5, 6 1)         |
|b  |POINT (1 2)                   |
|c  |POLYGON ((7 1, 7 3, 9 3, 7 1))|
+---+------------------------------+
```

You don’t see the SRID when printing the Sedona DataFrame, but this metadata is maintained internally.

## Advantages of CSV for data with geometry

There are a few advantages of using CSV with geometry data:

* Many engines support CSV
* It’s human-readable
* The “extended” format saves CRS information
* The standard has withstood the test of time

But CSV also has lots of disadvantages.

## Disadvantages of CSV for datasets with geometry

Here are the disadvantages of storing geometric data in CSV files:

* CSV is a row-oriented file format, so engines can’t cherry-pick individual columns while reading data.  Column-oriented files allow for column pruning, an important performance feature.
* CSV’s row-oriented nature makes it harder to compress files.
* CSV files don’t contain the schema of the data so engines need to either infer the schema or users need to manually specify it when reading the data.  Inferring the schema is error-prone, and manually specifying the schema is tedious.
* CSV doesn’t store row-group metadata, so row-group skipping isn’t possible.
* CSV doesn’t store file-level metadata, so file skipping isn’t possible.
* When SRID metadata is tracked, it’s written on every line of the CSV file, which unnecessarily takes up a lot of space because CSVs don’t support file-level metadata.

## Conclusion

Spark and Sedona support the CSV file format for geometric data, but it generally is slow and should only be used when necessary.

If you’re building a geospatial data lake, GeoParquet is almost always a better alternative.

And if you’re building a geospatial data lakehouse, then Iceberg is a good option.
