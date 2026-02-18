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

## Explanation

* sedona.global.index
	* Use spatial index (currently, only supports in SQL range join and SQL distance join)
	* Default: true
	* Possible values: true, false
* sedona.global.indextype
	* Spatial index type, only valid when "sedona.global.index" is true
	* Default: rtree
	* Possible values: rtree, quadtree
* sedona.join.autoBroadcastJoinThreshold
	* Configures the maximum size in bytes for a table that will be broadcast to all worker nodes when performing a join.
      By setting this value to -1 automatic broadcasting can be disabled.
	* Default: The default value is the same as spark.sql.autoBroadcastJoinThreshold
	* Possible values: any integer with a byte suffix i.e. 10MB or 512KB
* sedona.join.gridtype
	* Spatial partitioning grid type for join query
	* Default: kdbtree
	* Possible values: quadtree, kdbtree
* spark.sedona.join.knn.includeTieBreakers
	* KNN join will include all ties in the result, possibly returning more than k results
	* Default: false
	* Possible values: true, false
* sedona.join.indexbuildside **(Advanced users only!)**
	* The side which Sedona builds spatial indices on
	* Default: left
	* Possible values: left, right
* sedona.join.numpartition **(Advanced users only!)**
	* Number of partitions for both sides in a join query
	* Default: -1, which means use the existing partitions
	* Possible values: any integers
* sedona.join.spatitionside **(Advanced users only!)**
	* The dominant side in spatial partitioning stage
	* Default: left
	* Possible values: left, right
* sedona.join.optimizationmode **(Advanced users only!)**
	* When should Sedona optimize spatial join SQL queries
	* Default: nonequi
	* Possible values:
		* all: Always optimize spatial join queries, even for equi-joins.
		* none: Disable optimization for spatial joins.
		* nonequi: Optimize spatial join queries that are not equi-joins.
* spark.sedona.enableParserExtension
	* Enable the parser extension to parse GEOMETRY data type in SQL DDL statements
	* Default: true
	* Possible values: true, false

## CRS Transformation

* spark.sedona.crs.geotools
	* Controls which library is used for CRS transformations in ST_Transform
	* Default: raster
	* Possible values:
		* none: Use proj4sedona for all transformations
		* raster: Use proj4sedona for vector transformations, GeoTools for raster transformations
		* all: Use GeoTools for all transformations (legacy behavior)
	* Since: v1.9.0
* spark.sedona.crs.url.base
	* Base URL of a CRS definition server for resolving authority codes (e.g., EPSG) via HTTP. When set, ST_Transform will consult this URL provider before the built-in definitions.
	* Default: (empty string — URL provider disabled)
	* Example: `https://crs.example.com`
	* Since: v1.9.0
* spark.sedona.crs.url.pathTemplate
	* URL path template appended to `spark.sedona.crs.url.base`. The placeholders `{authority}` and `{code}` are replaced with the authority name (e.g., `epsg`) and numeric code (e.g., `4326`) at runtime.
	* Default: `/{authority}/{code}.json`
	* Example: `/epsg/{code}.json` (for a server that only serves EPSG codes)
	* Since: v1.9.0
* spark.sedona.crs.url.format
	* The CRS definition format returned by the URL provider.
	* Default: projjson
	* Possible values:
		* projjson: PROJJSON format
		* proj: PROJ string format
		* wkt1: OGC WKT1 format
		* wkt2: ISO 19162 WKT2 format
	* Since: v1.9.0
