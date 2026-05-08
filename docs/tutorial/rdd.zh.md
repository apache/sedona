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

本页介绍如何使用 Sedona-core 创建空间 RDD（SpatialRDD）并执行空间查询。

## 配置依赖

请参阅 [配置依赖](sql.md#set-up-dependencies) 进行依赖配置。

## 创建 Sedona 配置

请参阅 [创建 Sedona 配置](sql.md#create-sedona-config) 创建 Sedona 配置。

## 初始化 SedonaContext

请参阅 [初始化 SedonaContext](sql.md#initiate-sedonacontext) 初始化 SedonaContext。

## 从 SedonaSQL DataFrame 创建 SpatialRDD

请先按 [创建 Geometry 类型列](sql.md#create-a-geometry-type-column) 创建 Geometry 类型列，然后从 DataFrame 创建 SpatialRDD：

=== "Scala"

	```scala
	var spatialRDD = StructuredAdapter.toSpatialRdd(spatialDf, "usacounty")
	```

=== "Java"

	```java
	SpatialRDD spatialRDD = StructuredAdapter.toSpatialRdd(spatialDf, "usacounty")
	```

=== "Python"

	```python
	from sedona.spark import StructuredAdapter

	spatialRDD = StructuredAdapter.toSpatialRdd(spatialDf, "usacounty")
	```

`"usacounty"` 是几何列的列名，可选参数。如果不提供，将使用第一个几何列。

## 转换坐标参考系

Sedona 不会自动管理 SpatialRDD 中所有几何对象的坐标单位（基于度还是基于米）。SedonaSQL 中所有相关距离的单位与 SpatialRDD 中几何对象的单位保持一致。

自 `v1.5.0` 起，该函数默认使用 lon/lat 顺序（之前为 lat/lon 顺序）。可以使用 ==spatialRDD.flipCoordinates== 交换 X 与 Y。

转换 SpatialRDD 的坐标参考系：

=== "Scala"

	```scala
	val sourceCrsCode = "epsg:4326" // WGS84，最常见的基于度的 CRS
	val targetCrsCode = "epsg:3857" // 最常见的基于米的 CRS
	objectRDD.CRSTransform(sourceCrsCode, targetCrsCode, false)
	```

=== "Java"

	```java
	String sourceCrsCode = "epsg:4326" // WGS84，最常见的基于度的 CRS
	String targetCrsCode = "epsg:3857" // 最常见的基于米的 CRS
	objectRDD.CRSTransform(sourceCrsCode, targetCrsCode, false)
	```

=== "Python"

	```python
	sourceCrsCode = "epsg:4326" // WGS84，最常见的基于度的 CRS
	targetCrsCode = "epsg:3857" // 最常见的基于米的 CRS
	objectRDD.CRSTransform(sourceCrsCode, targetCrsCode, False)
	```

`CRSTransform(sourceCrsCode, targetCrsCode, false)` 的第三个参数为 `false` 表示不容忍 Datum shift；如果希望宽松处理，请改为 `true`。

!!!warning
	CRS 转换应在创建每个 SpatialRDD 之后立即进行，否则会导致查询结果错误。例如：

=== "Scala"

	```scala
	val objectRDD = WktReader.readToGeometryRDD(sedona.sparkContext, inputLocation, wktColumn, allowTopologyInvalidGeometries, skipSyntaxInvalidGeometries)
	objectRDD.CRSTransform("epsg:4326", "epsg:3857", false)
	```

=== "Java"

	```java
	SpatialRDD objectRDD = WktReader.readToGeometryRDD(sedona.sparkContext, inputLocation, wktColumn, allowTopologyInvalidGeometries, skipSyntaxInvalidGeometries)
	objectRDD.CRSTransform("epsg:4326", "epsg:3857", false)
	```

=== "Python"

	```python
	objectRDD = WktReader.readToGeometryRDD(sedona.sparkContext, inputLocation, wktColumn, allowTopologyInvalidGeometries, skipSyntaxInvalidGeometries)
	objectRDD.CRSTransform("epsg:4326", "epsg:3857", False)
	```

详细的 CRS 信息可在 [EPSG.io](https://epsg.io/) 查询。

## 编写空间范围查询

空间范围查询接收一个查询窗口与一个 SpatialRDD，返回与查询窗口满足指定关系的所有几何对象。

假设您已经有一个 SpatialRDD（typed 或 generic），可以用以下代码执行空间范围查询。

==spatialPredicate== 可以设置为 `SpatialPredicate.INTERSECTS`，返回与查询窗口相交的所有几何对象。支持的空间谓词包括：

* `CONTAINS`：几何对象完全位于查询窗口内部
* `INTERSECTS`：几何对象与查询窗口至少有一个公共点
* `WITHIN`：几何对象完全位于查询窗口之内（不接触边）
* `COVERS`：查询窗口的所有点都在几何对象上
* `COVERED_BY`：几何对象的所有点都在查询窗口上
* `OVERLAPS`：几何对象与查询窗口在空间上相互重叠
* `CROSSES`：几何对象与查询窗口在空间上相交穿过
* `TOUCHES`：几何对象与查询窗口仅在边界上有公共点
* `EQUALS`：几何对象与查询窗口在空间上相等

!!!note
	空间范围查询等价于带空间谓词作为搜索条件的 SELECT。例如：
	```sql
	SELECT *
	FROM checkin
	WHERE ST_Intersects(checkin.location, queryWindow)
	```

=== "Scala"

	```scala
	val rangeQueryWindow = new Envelope(-90.01, -80.01, 30.01, 40.01)
	val spatialPredicate = SpatialPredicate.COVERED_BY // 仅返回完全被窗口覆盖的几何对象
	val usingIndex = false
	var queryResult = RangeQuery.SpatialRangeQuery(spatialRDD, rangeQueryWindow, spatialPredicate, usingIndex)
	```

=== "Java"

	```java
	Envelope rangeQueryWindow = new Envelope(-90.01, -80.01, 30.01, 40.01)
	SpatialPredicate spatialPredicate = SpatialPredicate.COVERED_BY // 仅返回完全被窗口覆盖的几何对象
	boolean usingIndex = false
	JavaRDD queryResult = RangeQuery.SpatialRangeQuery(spatialRDD, rangeQueryWindow, spatialPredicate, usingIndex)
	```

=== "Python"

	```python
	from sedona.spark import Envelope
	from sedona.spark import RangeQuery

	range_query_window = Envelope(-90.01, -80.01, 30.01, 40.01)
	consider_boundary_intersection = False  ## 仅返回完全被窗口覆盖的几何对象
	using_index = False
	query_result = RangeQuery.SpatialRangeQuery(spatial_rdd, range_query_window, consider_boundary_intersection, using_index)
	```

!!!note
    Sedona Python 用户：如果希望在转换为 Spatial DataFrame 时避免 jvm-python serde，请改用同一模块下的 `RangeQueryRaw`。其参数与 `RangeQuery` 相同，但返回的是 jvm rdd 引用，可由 Adapter 不经 Python-jvm serde 直接转换为 DataFrame。

    示例：
    ```python
    from sedona.spark import Envelope
    from sedona.spark import RangeQueryRaw
    from sedona.spark import Adapter

    range_query_window = Envelope(-90.01, -80.01, 30.01, 40.01)
    consider_boundary_intersection = False  ## 仅返回完全被窗口覆盖的几何对象
    using_index = False
    query_result = RangeQueryRaw.SpatialRangeQuery(
        spatial_rdd, range_query_window, consider_boundary_intersection, using_index
    )
    gdf = StructuredAdapter.toDf(query_result, spark, ["col1", ..., "coln"])
    ```

### 范围查询窗口

除了矩形（Envelope）类型之外，Sedona 范围查询窗口还可以是 Point/Polygon/LineString。

构造一个点、4 顶点折线、4 顶点多边形的代码如下：

=== "Scala"

	```scala
	val geometryFactory = new GeometryFactory()
	val pointObject = geometryFactory.createPoint(new Coordinate(-84.01, 34.01))

	val geometryFactory = new GeometryFactory()
	val coordinates = new Array[Coordinate](5)
	coordinates(0) = new Coordinate(0,0)
	coordinates(1) = new Coordinate(0,4)
	coordinates(2) = new Coordinate(4,4)
	coordinates(3) = new Coordinate(4,0)
	coordinates(4) = coordinates(0) // 末尾坐标与首坐标相同以构成闭环
	val polygonObject = geometryFactory.createPolygon(coordinates)

	val geometryFactory = new GeometryFactory()
	val coordinates = new Array[Coordinate](4)
	coordinates(0) = new Coordinate(0,0)
	coordinates(1) = new Coordinate(0,4)
	coordinates(2) = new Coordinate(4,4)
	coordinates(3) = new Coordinate(4,0)
	val linestringObject = geometryFactory.createLineString(coordinates)
	```

=== "Java"

	```java
	GeometryFactory geometryFactory = new GeometryFactory()
	Point pointObject = geometryFactory.createPoint(new Coordinate(-84.01, 34.01))

	GeometryFactory geometryFactory = new GeometryFactory()
	Coordinate[] coordinates = new Array[Coordinate](5)
	coordinates(0) = new Coordinate(0,0)
	coordinates(1) = new Coordinate(0,4)
	coordinates(2) = new Coordinate(4,4)
	coordinates(3) = new Coordinate(4,0)
	coordinates(4) = coordinates(0) // 末尾坐标与首坐标相同以构成闭环
	Polygon polygonObject = geometryFactory.createPolygon(coordinates)

	GeometryFactory geometryFactory = new GeometryFactory()
	val coordinates = new Array[Coordinate](4)
	coordinates(0) = new Coordinate(0,0)
	coordinates(1) = new Coordinate(0,4)
	coordinates(2) = new Coordinate(4,4)
	coordinates(3) = new Coordinate(4,0)
	LineString linestringObject = geometryFactory.createLineString(coordinates)
	```

=== "Python"

	可以将 Shapely 几何对象作为查询窗口。Shapely 几何对象的创建方法请参阅 [Shapely 官方文档](https://shapely.readthedocs.io/en/stable/manual.html)。

### 使用空间索引

Sedona 提供两种空间索引：Quad-Tree 与 R-Tree。指定索引类型后，Sedona 会在每个 SpatialRDD 分区上构建本地索引。

在空间范围查询中使用空间索引：

=== "Scala"

	```scala
	val rangeQueryWindow = new Envelope(-90.01, -80.01, 30.01, 40.01)
	val spatialPredicate = SpatialPredicate.COVERED_BY // 仅返回完全被窗口覆盖的几何对象

	val buildOnSpatialPartitionedRDD = false // 仅在执行 join 查询时设为 TRUE
	spatialRDD.buildIndex(IndexType.QUADTREE, buildOnSpatialPartitionedRDD)

	val usingIndex = true
	var queryResult = RangeQuery.SpatialRangeQuery(spatialRDD, rangeQueryWindow, spatialPredicate, usingIndex)
	```

=== "Java"

	```java
	Envelope rangeQueryWindow = new Envelope(-90.01, -80.01, 30.01, 40.01)
	SpatialPredicate spatialPredicate = SpatialPredicate.COVERED_BY // 仅返回完全被窗口覆盖的几何对象

	boolean buildOnSpatialPartitionedRDD = false // 仅在执行 join 查询时设为 TRUE
	spatialRDD.buildIndex(IndexType.QUADTREE, buildOnSpatialPartitionedRDD)

	boolean usingIndex = true
	JavaRDD queryResult = RangeQuery.SpatialRangeQuery(spatialRDD, rangeQueryWindow, spatialPredicate, usingIndex)
	```

=== "Python"

	```python
	from sedona.spark import Envelope
	from sedona.spark import IndexType
	from sedona.spark import RangeQuery

	range_query_window = Envelope(-90.01, -80.01, 30.01, 40.01)
	consider_boundary_intersection = False ## 仅返回完全被窗口覆盖的几何对象

	build_on_spatial_partitioned_rdd = False ## 仅在执行 join 查询时设为 TRUE
	spatial_rdd.buildIndex(IndexType.QUADTREE, build_on_spatial_partitioned_rdd)

	using_index = True

	query_result = RangeQuery.SpatialRangeQuery(
	    spatial_rdd,
	    range_query_window,
	    consider_boundary_intersection,
	    using_index
	)
	```

!!!tip
	不一定每次都使用索引，因为构建索引本身也会花时间。空间索引在数据为复杂多边形、折线时尤其有用。

### 输出格式

=== "Scala/Java"

	空间范围查询的输出格式仍然是一个 SpatialRDD。

=== "Python"

	空间范围查询返回另一个 RDD，元素为 GeoData 对象。

	`SpatialRangeQuery` 的结果可以像普通 Spark RDD 一样使用 `map` 等操作；通过 `collect` 收集后则成为 Python 对象列表。
	示例：

	```python
	query_result.map(lambda x: x.geom.length).collect()
	```

	```
	[
	 1.5900840000000045,
	 1.5906639999999896,
	 1.1110299999999995,
	 1.1096700000000084,
	 1.1415619999999933,
	 1.1386399999999952,
	 1.1415619999999933,
	 1.1418860000000137,
	 1.1392780000000045,
	 ...
	]
	```

	也可以转换为 GeoPandas GeoDataFrame：

	```python
	import geopandas as gpd
	gpd.GeoDataFrame(
	    query_result.map(lambda x: [x.geom, x.userData]).collect(),
	    columns=["geom", "user_data"],
	    geometry="geom"
	)
	```

## 编写空间 KNN 查询

空间 K 最近邻（KNN）查询接收 K、查询点与一个 SpatialRDD，返回 RDD 中距查询点最近的 K 个几何对象。

假设您已经有一个 SpatialRDD（typed 或 generic），可以用以下代码执行空间 KNN 查询：

=== "Scala"

	```scala
	val geometryFactory = new GeometryFactory()
	val pointObject = geometryFactory.createPoint(new Coordinate(-84.01, 34.01))
	val K = 1000 // K 个最近邻
	val usingIndex = false
	val result = KNNQuery.SpatialKnnQuery(objectRDD, pointObject, K, usingIndex)
	```

=== "Java"

	```java
	GeometryFactory geometryFactory = new GeometryFactory()
	Point pointObject = geometryFactory.createPoint(new Coordinate(-84.01, 34.01))
	int K = 1000 // K 个最近邻
	boolean usingIndex = false
	JavaRDD result = KNNQuery.SpatialKnnQuery(objectRDD, pointObject, K, usingIndex)
	```

=== "Python"

	```python
	from sedona.spark import KNNQuery
	from shapely.geometry import Point

	point = Point(-84.01, 34.01)
	k = 1000 ## K 个最近邻
	using_index = False
	result = KNNQuery.SpatialKnnQuery(object_rdd, point, k, using_index)
	```

!!!note
	返回 5 个最近邻的空间 KNN 查询等价于以下 Spatial SQL：
	```sql
	SELECT ck.name, ck.rating, ST_Distance(ck.location, myLocation) AS distance
	FROM checkins ck
	ORDER BY distance DESC
	LIMIT 5
	```

### 查询中心几何

除了 Point，Sedona 的 KNN 查询中心也可以是 Polygon 与 LineString。

=== "Scala/Java"

	创建 Polygon 与 LineString 的方法见 [范围查询窗口](#range-query-window)。

=== "Python"

	创建 Polygon 或 LineString 请参阅 [Shapely 官方文档](https://shapely.readthedocs.io/en/stable/manual.html)。

### 使用空间索引

在空间 KNN 查询中使用空间索引：

=== "Scala"

	```scala
	val geometryFactory = new GeometryFactory()
	val pointObject = geometryFactory.createPoint(new Coordinate(-84.01, 34.01))
	val K = 1000 // K 个最近邻


	val buildOnSpatialPartitionedRDD = false // 仅在执行 join 查询时设为 TRUE
	objectRDD.buildIndex(IndexType.RTREE, buildOnSpatialPartitionedRDD)

	val usingIndex = true
	val result = KNNQuery.SpatialKnnQuery(objectRDD, pointObject, K, usingIndex)
	```

=== "Java"

	```java
	GeometryFactory geometryFactory = new GeometryFactory()
	Point pointObject = geometryFactory.createPoint(new Coordinate(-84.01, 34.01))
	val K = 1000 // K 个最近邻


	boolean buildOnSpatialPartitionedRDD = false // 仅在执行 join 查询时设为 TRUE
	objectRDD.buildIndex(IndexType.RTREE, buildOnSpatialPartitionedRDD)

	boolean usingIndex = true
	JavaRDD result = KNNQuery.SpatialKnnQuery(objectRDD, pointObject, K, usingIndex)
	```

=== "Python"

	```python
	from sedona.spark import KNNQuery
	from sedona.spark import IndexType
	from shapely.geometry import Point

	point = Point(-84.01, 34.01)
	k = 5 ## K 个最近邻

	build_on_spatial_partitioned_rdd = False ## 仅在执行 join 查询时设为 TRUE
	spatial_rdd.buildIndex(IndexType.RTREE, build_on_spatial_partitioned_rdd)

	using_index = True
	result = KNNQuery.SpatialKnnQuery(spatial_rdd, point, k, using_index)
	```

!!!warning
	空间 KNN 查询仅支持 R-Tree 索引。

### 输出格式

=== "Scala/Java"

	空间 KNN 查询的输出格式是一个几何对象列表，列表中包含 K 个几何对象。

=== "Python"

	输出格式是 GeoData 对象列表，包含 K 个 GeoData 对象。

	示例：
	```python
	>> result

	[GeoData, GeoData, GeoData, GeoData, GeoData]
	```

## 编写空间连接查询

空间连接查询接收两个 SpatialRDD A 与 B。对于 A 中的每个几何对象，从 B 中找出与之 covered/intersected 的几何对象。A 与 B 可以是任意几何类型，且类型不必相同。

假设您已经有两个 SpatialRDD（typed 或 generic），可以用以下代码执行空间连接查询：

=== "Scala"

	```scala
	val spatialPredicate = SpatialPredicate.COVERED_BY // 仅返回完全被 queryWindowRDD 中各窗口覆盖的几何对象
	val usingIndex = false

	objectRDD.analyze()

	objectRDD.spatialPartitioning(GridType.KDBTREE)
	queryWindowRDD.spatialPartitioning(objectRDD.getPartitioner)

	val result = JoinQuery.SpatialJoinQuery(objectRDD, queryWindowRDD, usingIndex, spatialPredicate)
	```

=== "Java"

	```java
	SpatialPredicate spatialPredicate = SpatialPredicate.COVERED_BY // 仅返回完全被 queryWindowRDD 中各窗口覆盖的几何对象
	val usingIndex = false

	objectRDD.analyze()

	objectRDD.spatialPartitioning(GridType.KDBTREE)
	queryWindowRDD.spatialPartitioning(objectRDD.getPartitioner)

	JavaPairRDD result = JoinQuery.SpatialJoinQuery(objectRDD, queryWindowRDD, usingIndex, spatialPredicate)
	```

=== "Python"

	```python
	from sedona.spark import GridType
	from sedona.spark import JoinQuery

	consider_boundary_intersection = False ## 仅返回完全被 queryWindowRDD 中各窗口覆盖的几何对象
	using_index = False

	object_rdd.analyze()

	object_rdd.spatialPartitioning(GridType.KDBTREE)
	query_window_rdd.spatialPartitioning(object_rdd.getPartitioner())

	result = JoinQuery.SpatialJoinQuery(object_rdd, query_window_rdd, using_index, consider_boundary_intersection)
	```

!!!note
	空间连接查询等价于以下 Spatial SQL：
	```sql
	SELECT superhero.name
	FROM city, superhero
	WHERE ST_Contains(city.geom, superhero.geom);
	```
	找出每座城市内的所有超级英雄。

### 使用空间分区

Sedona 的空间分区方法可以显著加速连接查询。可选的空间分区方式有 KDB-Tree、Quad-Tree 与 R-Tree。两个 SpatialRDD 必须使用相同方式分区。

如果先对 A 分区，则必须使用 A 的分区器对 B 分区。

=== "Scala/Java"

	```scala
	objectRDD.spatialPartitioning(GridType.KDBTREE)
	queryWindowRDD.spatialPartitioning(objectRDD.getPartitioner)
	```

=== "Python"

	```python
	object_rdd.spatialPartitioning(GridType.KDBTREE)
	query_window_rdd.spatialPartitioning(object_rdd.getPartitioner())
	```

或者：

=== "Scala/Java"

	```scala
	queryWindowRDD.spatialPartitioning(GridType.KDBTREE)
	objectRDD.spatialPartitioning(queryWindowRDD.getPartitioner)
	```

=== "Python"

	```python
	query_window_rdd.spatialPartitioning(GridType.KDBTREE)
	object_rdd.spatialPartitioning(query_window_rdd.getPartitioner())
	```

### 使用空间索引

在空间连接查询中使用空间索引：

=== "Scala"

	```scala
	objectRDD.spatialPartitioning(joinQueryPartitioningType)
	queryWindowRDD.spatialPartitioning(objectRDD.getPartitioner)

	val buildOnSpatialPartitionedRDD = true // 仅在执行 join 查询时设为 TRUE
	val usingIndex = true
	queryWindowRDD.buildIndex(IndexType.QUADTREE, buildOnSpatialPartitionedRDD)

	val result = JoinQuery.SpatialJoinQueryFlat(objectRDD, queryWindowRDD, usingIndex, spatialPredicate)
	```

=== "Java"

	```java
	objectRDD.spatialPartitioning(joinQueryPartitioningType)
	queryWindowRDD.spatialPartitioning(objectRDD.getPartitioner)

	boolean buildOnSpatialPartitionedRDD = true // 仅在执行 join 查询时设为 TRUE
	boolean usingIndex = true
	queryWindowRDD.buildIndex(IndexType.QUADTREE, buildOnSpatialPartitionedRDD)

	JavaPairRDD result = JoinQuery.SpatialJoinQueryFlat(objectRDD, queryWindowRDD, usingIndex, spatialPredicate)
	```

=== "Python"

	```python
	from sedona.spark import GridType
	from sedona.spark import IndexType
	from sedona.spark import JoinQuery

	object_rdd.spatialPartitioning(GridType.KDBTREE)
	query_window_rdd.spatialPartitioning(object_rdd.getPartitioner())

	build_on_spatial_partitioned_rdd = True ## 仅在执行 join 查询时设为 TRUE
	using_index = True
	query_window_rdd.buildIndex(IndexType.QUADTREE, build_on_spatial_partitioned_rdd)

	result = JoinQuery.SpatialJoinQueryFlat(object_rdd, query_window_rdd, using_index, True)
	```

索引应当只在两个 SpatialRDD 中的一个上构建，一般建议在较大的那一个上构建。

### 输出格式

=== "Scala/Java"

	空间连接查询的输出格式是一个 PairRDD，每个元素是一对几何对象：左侧来自 objectRDD，右侧来自 queryWindowRDD。

	```
	Point,Polygon
	Point,Polygon
	Point,Polygon
	Polygon,Polygon
	LineString,LineString
	Polygon,LineString
	...
	```

	左侧对象会被右侧对象 covered/intersected。

=== "Python"

	该查询的结果是一个 RDD，元素为长度为 2 的 GeoData 列表。
	示例：
	```python
	result.collect()
	```

	```
	[[GeoData, GeoData], [GeoData, GeoData] ...]
	```

	可以对结果做 RDD 操作，例如取多边形质心：
	```python
	result.map(lambda x: x[0].geom.centroid).collect()
	```

!!!note
    Sedona Python 用户：以下方法建议使用同一模块下的 `JoinQueryRaw`：

    - spatialJoin

    - DistanceJoinQueryFlat

    - SpatialJoinQueryFlat

    通过 Adapter 转换 DataFrame 时性能更好。这种方式可以避免 Python 与 jvm 之间昂贵的序列化，使您直接操作 Python 对象而不是原生几何。

    示例：
    ```python
    from sedona.spark import CircleRDD
    from sedona.spark import GridType
    from sedona.spark import JoinQueryRaw
    from sedona.spark import StructuredAdapter

    object_rdd.analyze()

    circle_rdd = CircleRDD(object_rdd, 0.1)  ## 用给定距离创建 CircleRDD
    circle_rdd.analyze()

    circle_rdd.spatialPartitioning(GridType.KDBTREE)
    spatial_rdd.spatialPartitioning(circle_rdd.getPartitioner())

    consider_boundary_intersection = (
        False  ## 仅返回完全被 queryWindowRDD 中各窗口覆盖的几何对象
    )
    using_index = False

    result = JoinQueryRaw.DistanceJoinQueryFlat(
        spatial_rdd, circle_rdd, using_index, consider_boundary_intersection
    )

    gdf = StructuredAdapter.toDf(
        result, ["left_col1", ..., "lefcoln"], ["rightcol1", ..., "rightcol2"], spark
    )
    ```

## 编写距离连接查询

!!!warning
	RDD 距离连接仅在点几何上稳定可靠。其他几何类型请使用 Spatial SQL。

距离连接查询接收两个 SpatialRDD A、B 与一个距离参数。对 A 中每个几何对象，从 B 中找出与其距离不超过给定距离的几何对象。A 与 B 可以是任意几何类型，且类型不必相同。距离的单位说明见 [此处](#transform-the-coordinate-reference-system)。

如果不希望转换数据，且可以接受查询精度的损失，可以使用近似的角度作为距离。可借助 [此换算工具](https://lucidar.me/en/online-unit-converter-length-to-angle/convert-degrees-to-meters/#online-converter)。

假设您已经有两个 SpatialRDD（typed 或 generic），可以用以下代码执行距离连接查询：

=== "Scala"

	```scala
	objectRddA.analyze()

	val circleRDD = new CircleRDD(objectRddA, 0.1) // 用给定距离创建 CircleRDD

	circleRDD.spatialPartitioning(GridType.KDBTREE)
	objectRddB.spatialPartitioning(circleRDD.getPartitioner)

	val spatialPredicate = SpatialPredicate.COVERED_BY // 仅返回完全被各查询窗口覆盖的几何对象
	val usingIndex = false

	val result = JoinQuery.DistanceJoinQueryFlat(objectRddB, circleRDD, usingIndex, spatialPredicate)
	```

=== "Java"

	```java
	objectRddA.analyze()

	CircleRDD circleRDD = new CircleRDD(objectRddA, 0.1) // 用给定距离创建 CircleRDD

	circleRDD.spatialPartitioning(GridType.KDBTREE)
	objectRddB.spatialPartitioning(circleRDD.getPartitioner)

	SpatialPredicate spatialPredicate = SpatialPredicate.COVERED_BY // 仅返回完全被各查询窗口覆盖的几何对象
	boolean usingIndex = false

	JavaPairRDD result = JoinQuery.DistanceJoinQueryFlat(objectRddB, circleRDD, usingIndex, spatialPredicate)
	```

=== "Python"

	```python
	from sedona.spark import CircleRDD
	from sedona.spark import GridType
	from sedona.spark import JoinQuery

	object_rdd.analyze()

	circle_rdd = CircleRDD(object_rdd, 0.1) ## 用给定距离创建 CircleRDD
	circle_rdd.analyze()

	circle_rdd.spatialPartitioning(GridType.KDBTREE)
	spatial_rdd.spatialPartitioning(circle_rdd.getPartitioner())

	consider_boundary_intersection = False ## 仅返回完全被各查询窗口覆盖的几何对象
	using_index = False

	result = JoinQuery.DistanceJoinQueryFlat(spatial_rdd, circle_rdd, using_index, consider_boundary_intersection)
	```

距离连接仅支持 `COVERED_BY` 与 `INTERSECTS` 两种空间谓词。其他部分与空间连接查询相同。

空间分区的细节见 [此处](#use-spatial-partitioning)。

在连接查询中使用空间索引的细节见 [此处](#use-spatial-indexes_2)。

距离连接查询的输出格式见 [此处](#output-format_2)。

!!!note
	距离连接查询等价于以下 Spatial SQL：
	```sql
	SELECT superhero.name
	FROM city, superhero
	WHERE ST_Distance(city.geom, superhero.geom) <= 10;
	```
	找出与每座城市相距不超过 10 英里的超级英雄。

## 保存到永久存储

可以随时把 SpatialRDD 写回到 HDFS、Amazon S3 等永久存储。

### 保存 SpatialRDD（未建索引）

把 SpatialRDD 保存为分布式 object file：

=== "Scala/Java"

	```scala
	objectRDD.rawSpatialRDD.saveAsObjectFile("hdfs://PATH")
	```

=== "Python"

	```python
	object_rdd.rawJvmSpatialRDD.saveAsObjectFile("hdfs://PATH")
	```

!!!note
	分布式 object file 中的每个对象都是字节数组（不可读）。这是 Geometry 或 SpatialIndex 的序列化格式。

### 保存 SpatialRDD（已建索引）

已建索引的 typed SpatialRDD 与 generic SpatialRDD 都可以保存到永久存储，但已建索引的 SpatialRDD 必须保存为分布式 object file。

```
objectRDD.indexedRawRDD.saveAsObjectFile("hdfs://PATH")
```

### 保存 SpatialRDD（已分区，未建索引）

经过空间分区的 RDD 可以保存到永久存储，但 Spark 无法保留原 RDD 的分区 ID。这会导致 join 查询结果错误，相关解决方案仍在开发中，敬请关注！

### 重新加载已保存的 SpatialRDD

您可以轻松重新加载保存为==分布式 object file==的 SpatialRDD：

=== "Scala"

	```scala
	var savedRDD = new SpatialRDD[Geometry]
	savedRDD.rawSpatialRDD = sc.objectFile[Geometry]("hdfs://PATH")
	```

=== "Java"

	```java
	SpatialRDD savedRDD = new SpatialRDD<Geometry>
	savedRDD.rawSpatialRDD = sc.objectFile<Geometry>("hdfs://PATH")
	```

=== "Python"

	```python
	saved_rdd = load_spatial_rdd_from_disc(sc, "hdfs://PATH", GeoType.GEOMETRY)
	```

重新加载已建索引的 SpatialRDD：

=== "Scala"

	```scala
	var savedRDD = new SpatialRDD[Geometry]
	savedRDD.indexedRawRDD = sc.objectFile[SpatialIndex]("hdfs://PATH")
	```

=== "Java"

	```java
	SpatialRDD savedRDD = new SpatialRDD<Geometry>
	savedRDD.indexedRawRDD = sc.objectFile<SpatialIndex>("hdfs://PATH")
	```

=== "Python"

	```python
	saved_rdd = SpatialRDD()
	saved_rdd.indexedRawRDD = load_spatial_index_rdd_from_disc(sc, "hdfs://PATH")
	```
