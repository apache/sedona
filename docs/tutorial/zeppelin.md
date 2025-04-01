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

Sedona provides a Helium visualization plugin tailored for [Apache Zeppelin](https://zeppelin.apache.org/). This finally bridges the gap between Sedona and Zeppelin. Please read [Install Sedona-Zeppelin](../setup/zeppelin.md) to learn how to install this plugin in Zeppelin.

Sedona-Zeppelin equips two approaches to visualize spatial data in Zeppelin. The first approach uses Zeppelin to plot all spatial objects on the map. The second one leverages SedonaViz to generate map images and overlay them on maps.

## Small-scale without SedonaViz

!!! danger
	Zeppelin is just a front-end visualization framework. This approach is not scalable and will fail at large-scale geospatial data. Please scroll down to read SedonaViz solution.

You can use Apache Zeppelin to plot a small number of spatial objects, such as 1000 points. Assume you already have a Spatial DataFrame, you need to convert the geometry column to WKT string column use the following command in your Zeppelin Spark notebook Scala paragraph:

```scala
spark.sql(
  """
    |CREATE OR REPLACE TEMP VIEW wktpoint AS
    |SELECT ST_AsText(shape) as geom
    |FROM pointtable
  """.stripMargin)
```

Then create an SQL paragraph to fetch the data

```sql
%sql
SELECT *
FROM wktpoint
```

Select the geometry column to visualize:

![Create an SQL paragraph to fetch the data and then select the geometry column](../image/sql-zeppelin.gif)

## Large-scale with SedonaViz

SedonaViz is a distributed visualization system that allows you to visualize big spatial data at scale. Please read [How to use SedonaViz](viz.md).

You can use Sedona-Zeppelin to ask Zeppelin to overlay SedonaViz images on a map background. This way, you can easily visualize 1 billion spatial objects or more (depends on your cluster size).

First, encode images of SedonaViz DataFrame in Zeppelin Spark notebook Scala paragraph,

```
spark.sql(
  """
    |CREATE OR REPLACE TEMP VIEW images AS
    |SELECT ST_EncodeImage(image) AS image, (SELECT ST_AsText(bound) FROM boundtable) AS boundary
    |FROM images
  """.stripMargin)
```

Then create an SQL paragraph to fetch the data

```sql
%sql
SELECT *, 'I am the map center!'
FROM images
```

Select the image and its geospatial boundary:

![Selecting the image and its boundary](../image/viz-zeppelin.gif)

## Zeppelin Spark notebook demo

We provide a full Zeppelin Spark notebook which demonstrates all functions. Please download [Sedona-Zeppelin notebook template](../image/geospark-zeppelin-demo.json) and [test data - arealm.csv](../image/arealm.csv).

You need to use Zeppelin to import this notebook JSON file and modify the input data path in the notebook.
