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

## Usage

SedonaSQL supports many parameters. To change their values,

1. Set it through SparkConf:

```scala
sparkSession = SparkSession.builder().
      config("spark.serializer","org.apache.spark.serializer.KryoSerializer").
      config("spark.kryo.registrator", "org.apache.sedona.core.serde.SedonaKryoRegistrator").
      config("sedona.global.index","true")
      master("local[*]").appName("mySedonaSQLdemo").getOrCreate()
```

2. Check your current SedonaSQL configuration:

```scala
val sedonaConf = new SedonaConf(sparkSession.conf)
println(sedonaConf)
```

3. Sedona parameters can be changed at runtime:

```scala
sparkSession.conf.set("sedona.global.index","false")
```

In addition, you can also add `spark` prefix to the parameter name, for example:

```scala
sparkSession.conf.set("spark.sedona.global.index","false")
```

However, any parameter set through `spark` prefix will be honored by Spark, which means you can set these parameters before hand via `spark-defaults.conf` or Spark on Kubernetes configuration.

If you set the same parameter through both `sedona` and `spark.sedona` prefixes, the parameter set through `sedona` prefix will override the parameter set through `spark.sedona` prefix.

## General Parameters

| Parameter | Description | Default | Possible Values |
| :--- | :--- | :--- | :--- |
| `sedona.global.index` | Use spatial index (currently, only supports in SQL range join and SQL distance join) | `true` | `true`, `false` |
| `sedona.global.indextype` | Spatial index type, only valid when `sedona.global.index` is true | `rtree` | `rtree`, `quadtree` |
| `spark.sedona.enableParserExtension` | Enable the parser extension to parse GEOMETRY data type in SQL DDL statements | `true` | `true`, `false` |

## Join Parameters

| Parameter | Description | Default | Possible Values |
| :--- | :--- | :--- | :--- |
| `sedona.join.autoBroadcastJoinThreshold` | Maximum size in bytes for a table that will be broadcast to all worker nodes when performing a join. Set to -1 to disable automatic broadcasting. | Same as `spark.sql.autoBroadcastJoinThreshold` | Any integer with a byte suffix, e.g. `10MB` or `512KB` |
| `sedona.join.gridtype` | Spatial partitioning grid type for join query | `kdbtree` | `quadtree`, `kdbtree` |
| `spark.sedona.join.knn.includeTieBreakers` | KNN join will include all ties in the result, possibly returning more than k results | `false` | `true`, `false` |
| `sedona.join.indexbuildside` | **(Advanced)** The side which Sedona builds spatial indices on | `left` | `left`, `right` |
| `sedona.join.numpartition` | **(Advanced)** Number of partitions for both sides in a join query | `-1` (use existing partitions) | Any integer |
| `sedona.join.spatitionside` | **(Advanced)** The dominant side in spatial partitioning stage | `left` | `left`, `right` |
| `sedona.join.optimizationmode` | **(Advanced)** When Sedona should optimize spatial join SQL queries | `nonequi` | `all` (always optimize, even equi-joins), `none` (disable optimization), `nonequi` (optimize non-equi-joins only) |

## CRS Transformation Parameters

| Parameter | Description | Default | Possible Values | Since |
| :--- | :--- | :--- | :--- | :--- |
| `spark.sedona.crs.geotools` | Controls which library is used for CRS transformations in ST_Transform | `raster` | `none` (proj4sedona for all), `raster` (proj4sedona for vector, GeoTools for raster), `all` (GeoTools for all — legacy) | v1.9.0 |
| `spark.sedona.crs.url.base` | Base URL of a CRS definition server for resolving authority codes (e.g., EPSG) via HTTP. When set, ST_Transform will consult this URL provider before the built-in definitions. | _(empty — disabled)_ | e.g. `https://crs.example.com` | v1.9.0 |
| `spark.sedona.crs.url.pathTemplate` | URL path template appended to `spark.sedona.crs.url.base`. Placeholders `{authority}` and `{code}` are replaced at runtime. | `/{authority}/{code}.json` | e.g. `/epsg/{code}.json` | v1.9.0 |
| `spark.sedona.crs.url.format` | The CRS definition format returned by the URL provider | `projjson` | `projjson`, `proj`, `wkt1` (OGC WKT1), `wkt2` (ISO 19162 WKT2) | v1.9.0 |
