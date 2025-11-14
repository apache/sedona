
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

The page outlines the steps to create Spatial RDDs and run spatial queries using Sedona-core.

## Set up dependencies

Please refer to [Set up dependencies](sql.md#set-up-dependencies) to set up dependencies.

## Create Sedona config

Please refer to [Create Sedona config](sql.md#create-sedona-config) to create a Sedona config.

## Initiate SedonaContext

Please refer to [Initiate SedonaContext](sql.md#initiate-sedonacontext) to initiate a SedonaContext.

## Create a SpatialRDD from SedonaSQL DataFrame

Please refer to [Create a Geometry type column](sql.md#create-a-geometry-type-column) to create a Geometry type column. Then you can create a SpatialRDD from the DataFrame.

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

"usacounty" is the name of the geometry column. It is an optional parameter. If you don't provide it, the first geometry column will be used.

## Transform the Coordinate Reference System

Sedona doesn't control the coordinate unit (degree-based or meter-based) of all geometries in an SpatialRDD. The unit of all related distances in Sedona is same as the unit of all geometries in an SpatialRDD.

By default, this function uses lon/lat order since `v1.5.0`. Before, it used lat/lon order. You can use ==spatialRDD.flipCoordinates== to swap X and Y.

To convert Coordinate Reference System of an SpatialRDD, use the following code:

=== "Scala"

	```scala
	val sourceCrsCode = "epsg:4326" // WGS84, the most common degree-based CRS
	val targetCrsCode = "epsg:3857" // The most common meter-based CRS
	objectRDD.CRSTransform(sourceCrsCode, targetCrsCode, false)
	```

=== "Java"

	```java
	String sourceCrsCode = "epsg:4326" // WGS84, the most common degree-based CRS
	String targetCrsCode = "epsg:3857" // The most common meter-based CRS
	objectRDD.CRSTransform(sourceCrsCode, targetCrsCode, false)
	```

=== "Python"

	```python
	sourceCrsCode = "epsg:4326" // WGS84, the most common degree-based CRS
	targetCrsCode = "epsg:3857" // The most common meter-based CRS
	objectRDD.CRSTransform(sourceCrsCode, targetCrsCode, False)
	```

`false` in CRSTransform(sourceCrsCode, targetCrsCode, false) means that it will not tolerate Datum shift. If you want it to be lenient, use `true` instead.

!!!warning
	CRS transformation should be done right after creating each SpatialRDD, otherwise it will lead to wrong query results. For instance, use something like this:

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

The details CRS information can be found on [EPSG.io](https://epsg.io/)

## Write a Spatial Range Query

A spatial range query takes as input a range query window and an SpatialRDD and returns all geometries that have specified relationship with the query window.

Assume you now have a SpatialRDD (typed or generic). You can use the following code to issue a Spatial Range Query on it.

==spatialPredicate== can be set to `SpatialPredicate.INTERSECTS` to return all geometries intersect with query window. Supported spatial predicates are:

* `CONTAINS`: geometry is completely inside the query window
* `INTERSECTS`: geometry have at least one point in common with the query window
* `WITHIN`: geometry is completely within the query window (no touching edges)
* `COVERS`: query window has no point outside of the geometry
* `COVERED_BY`: geometry has no point outside of the query window
* `OVERLAPS`: geometry and the query window spatially overlap
* `CROSSES`: geometry and the query window spatially cross
* `TOUCHES`: the only points shared between geometry and the query window are on the boundary of geometry and the query window
* `EQUALS`: geometry and the query window are spatially equal

!!!note
	Spatial range query is equivalent with a SELECT query with spatial predicate as search condition in Spatial SQL. An example query is as follows:
	```sql
	SELECT *
	FROM checkin
	WHERE ST_Intersects(checkin.location, queryWindow)
	```

=== "Scala"

	```scala
	val rangeQueryWindow = new Envelope(-90.01, -80.01, 30.01, 40.01)
	val spatialPredicate = SpatialPredicate.COVERED_BY // Only return gemeotries fully covered by the window
	val usingIndex = false
	var queryResult = RangeQuery.SpatialRangeQuery(spatialRDD, rangeQueryWindow, spatialPredicate, usingIndex)
	```

=== "Java"

	```java
	Envelope rangeQueryWindow = new Envelope(-90.01, -80.01, 30.01, 40.01)
	SpatialPredicate spatialPredicate = SpatialPredicate.COVERED_BY // Only return gemeotries fully covered by the window
	boolean usingIndex = false
	JavaRDD queryResult = RangeQuery.SpatialRangeQuery(spatialRDD, rangeQueryWindow, spatialPredicate, usingIndex)
	```

=== "Python"

	```python
	from sedona.spark import Envelope
	from sedona.spark import RangeQuery

	range_query_window = Envelope(-90.01, -80.01, 30.01, 40.01)
	consider_boundary_intersection = False  ## Only return gemeotries fully covered by the window
	using_index = False
	query_result = RangeQuery.SpatialRangeQuery(spatial_rdd, range_query_window, consider_boundary_intersection, using_index)
	```

!!!note
    Sedona Python users: Please use RangeQueryRaw from the same module if you want to avoid jvm python serde while converting to Spatial DataFrame. It takes the same parameters as RangeQuery but returns reference to jvm rdd which can be converted to dataframe without python - jvm serde using Adapter.

    Example:
    ```python
    from sedona.spark import Envelope
    from sedona.spark import RangeQueryRaw
    from sedona.spark import Adapter

    range_query_window = Envelope(-90.01, -80.01, 30.01, 40.01)
    consider_boundary_intersection = (
        False  ## Only return gemeotries fully covered by the window
    )
    using_index = False
    query_result = RangeQueryRaw.SpatialRangeQuery(
        spatial_rdd, range_query_window, consider_boundary_intersection, using_index
    )
    gdf = StructuredAdapter.toDf(query_result, spark, ["col1", ..., "coln"])
    ```

### Range query window

Besides the rectangle (Envelope) type range query window, Sedona range query window can be Point/Polygon/LineString.

The code to create a point, linestring (4 vertices) and polygon (4 vertices) is as follows:

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
	coordinates(4) = coordinates(0) // The last coordinate is the same as the first coordinate in order to compose a closed ring
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
	coordinates(4) = coordinates(0) // The last coordinate is the same as the first coordinate in order to compose a closed ring
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

	A Shapely geometry can be used as a query window. To create shapely geometries, please follow [Shapely official docs](https://shapely.readthedocs.io/en/stable/manual.html)

### Use spatial indexes

Sedona provides two types of spatial indexes, Quad-Tree and R-Tree. Once you specify an index type, Sedona will build a local tree index on each of the SpatialRDD partition.

To utilize a spatial index in a spatial range query, use the following code:

=== "Scala"

	```scala
	val rangeQueryWindow = new Envelope(-90.01, -80.01, 30.01, 40.01)
	val spatialPredicate = SpatialPredicate.COVERED_BY // Only return gemeotries fully covered by the window

	val buildOnSpatialPartitionedRDD = false // Set to TRUE only if run join query
	spatialRDD.buildIndex(IndexType.QUADTREE, buildOnSpatialPartitionedRDD)

	val usingIndex = true
	var queryResult = RangeQuery.SpatialRangeQuery(spatialRDD, rangeQueryWindow, spatialPredicate, usingIndex)
	```

=== "Java"

	```java
	Envelope rangeQueryWindow = new Envelope(-90.01, -80.01, 30.01, 40.01)
	SpatialPredicate spatialPredicate = SpatialPredicate.COVERED_BY // Only return gemeotries fully covered by the window

	boolean buildOnSpatialPartitionedRDD = false // Set to TRUE only if run join query
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
	consider_boundary_intersection = False ## Only return gemeotries fully covered by the window

	build_on_spatial_partitioned_rdd = False ## Set to TRUE only if run join query
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
	Using an index might not be the best choice all the time because building index also takes time. A spatial index is very useful when your data is complex polygons and line strings.

### Output format

=== "Scala/Java"

	The output format of the spatial range query is another SpatialRDD.

=== "Python"

	The output format of the spatial range query is another RDD which consists of GeoData objects.

	SpatialRangeQuery result can be used as RDD with map or other spark RDD functions. Also it can be used as
	Python objects when using collect method.
	Example:

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

	Or transformed to GeoPandas GeoDataFrame

	```python
	import geopandas as gpd
	gpd.GeoDataFrame(
	    query_result.map(lambda x: [x.geom, x.userData]).collect(),
	    columns=["geom", "user_data"],
	    geometry="geom"
	)
	```

## Write a Spatial KNN Query

A spatial K Nearest Neighbor query takes as input a K, a query point and a SpatialRDD and finds the K geometries in the RDD which are the closest to the query point.

Assume you now have a SpatialRDD (typed or generic). You can use the following code to issue a Spatial KNN Query on it.

=== "Scala"

	```scala
	val geometryFactory = new GeometryFactory()
	val pointObject = geometryFactory.createPoint(new Coordinate(-84.01, 34.01))
	val K = 1000 // K Nearest Neighbors
	val usingIndex = false
	val result = KNNQuery.SpatialKnnQuery(objectRDD, pointObject, K, usingIndex)
	```

=== "Java"

	```java
	GeometryFactory geometryFactory = new GeometryFactory()
	Point pointObject = geometryFactory.createPoint(new Coordinate(-84.01, 34.01))
	int K = 1000 // K Nearest Neighbors
	boolean usingIndex = false
	JavaRDD result = KNNQuery.SpatialKnnQuery(objectRDD, pointObject, K, usingIndex)
	```

=== "Python"

	```python
	from sedona.spark import KNNQuery
	from shapely.geometry import Point

	point = Point(-84.01, 34.01)
	k = 1000 ## K Nearest Neighbors
	using_index = False
	result = KNNQuery.SpatialKnnQuery(object_rdd, point, k, using_index)
	```

!!!note
	Spatial KNN query that returns 5 Nearest Neighbors is equal to the following statement in Spatial SQL
	```sql
	SELECT ck.name, ck.rating, ST_Distance(ck.location, myLocation) AS distance
	FROM checkins ck
	ORDER BY distance DESC
	LIMIT 5
	```

### Query center geometry

Besides the Point type, Sedona KNN query center can be Polygon and LineString.

=== "Scala/Java"

	To learn how to create Polygon and LineString object, see [Range query window](#range-query-window).

=== "Python"

	To create Polygon or Linestring object please follow [Shapely official docs](https://shapely.readthedocs.io/en/stable/manual.html)

### Use spatial indexes

To utilize a spatial index in a spatial KNN query, use the following code:

=== "Scala"

	```scala
	val geometryFactory = new GeometryFactory()
	val pointObject = geometryFactory.createPoint(new Coordinate(-84.01, 34.01))
	val K = 1000 // K Nearest Neighbors


	val buildOnSpatialPartitionedRDD = false // Set to TRUE only if run join query
	objectRDD.buildIndex(IndexType.RTREE, buildOnSpatialPartitionedRDD)

	val usingIndex = true
	val result = KNNQuery.SpatialKnnQuery(objectRDD, pointObject, K, usingIndex)
	```

=== "Java"

	```java
	GeometryFactory geometryFactory = new GeometryFactory()
	Point pointObject = geometryFactory.createPoint(new Coordinate(-84.01, 34.01))
	val K = 1000 // K Nearest Neighbors


	boolean buildOnSpatialPartitionedRDD = false // Set to TRUE only if run join query
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
	k = 5 ## K Nearest Neighbors

	build_on_spatial_partitioned_rdd = False ## Set to TRUE only if run join query
	spatial_rdd.buildIndex(IndexType.RTREE, build_on_spatial_partitioned_rdd)

	using_index = True
	result = KNNQuery.SpatialKnnQuery(spatial_rdd, point, k, using_index)
	```

!!!warning
	Only R-Tree index supports Spatial KNN query

### Output format

=== "Scala/Java"

	The output format of the spatial KNN query is a list of geometries. The list has K geometry objects.

=== "Python"

	The output format of the spatial KNN query is a list of GeoData objects.
	The list has K GeoData objects.

	Example:
	```python
	>> result

	[GeoData, GeoData, GeoData, GeoData, GeoData]
	```

## Write a Spatial Join Query

A spatial join query takes as input two Spatial RDD A and B. For each geometry in A, finds the geometries (from B) covered/intersected by it. A and B can be any geometry type and are not necessary to have the same geometry type.

Assume you now have two SpatialRDDs (typed or generic). You can use the following code to issue a Spatial Join Query on them.

=== "Scala"

	```scala
	val spatialPredicate = SpatialPredicate.COVERED_BY // Only return gemeotries fully covered by each query window in queryWindowRDD
	val usingIndex = false

	objectRDD.analyze()

	objectRDD.spatialPartitioning(GridType.KDBTREE)
	queryWindowRDD.spatialPartitioning(objectRDD.getPartitioner)

	val result = JoinQuery.SpatialJoinQuery(objectRDD, queryWindowRDD, usingIndex, spatialPredicate)
	```

=== "Java"

	```java
	SpatialPredicate spatialPredicate = SpatialPredicate.COVERED_BY // Only return gemeotries fully covered by each query window in queryWindowRDD
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

	consider_boundary_intersection = False ## Only return geometries fully covered by each query window in queryWindowRDD
	using_index = False

	object_rdd.analyze()

	object_rdd.spatialPartitioning(GridType.KDBTREE)
	query_window_rdd.spatialPartitioning(object_rdd.getPartitioner())

	result = JoinQuery.SpatialJoinQuery(object_rdd, query_window_rdd, using_index, consider_boundary_intersection)
	```

!!!note
	Spatial join query is equal to the following query in Spatial SQL:
	```sql
	SELECT superhero.name
	FROM city, superhero
	WHERE ST_Contains(city.geom, superhero.geom);
	```
	Find the superheroes in each city

### Use spatial partitioning

Sedona spatial partitioning method can significantly speed up the join query. Three spatial partitioning methods are available: KDB-Tree, Quad-Tree and R-Tree. Two SpatialRDD must be partitioned by the same way.

If you first partition SpatialRDD A, then you must use the partitioner of A to partition B.

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

Or

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

### Use spatial indexes

To utilize a spatial index in a spatial join query, use the following code:

=== "Scala"

	```scala
	objectRDD.spatialPartitioning(joinQueryPartitioningType)
	queryWindowRDD.spatialPartitioning(objectRDD.getPartitioner)

	val buildOnSpatialPartitionedRDD = true // Set to TRUE only if run join query
	val usingIndex = true
	queryWindowRDD.buildIndex(IndexType.QUADTREE, buildOnSpatialPartitionedRDD)

	val result = JoinQuery.SpatialJoinQueryFlat(objectRDD, queryWindowRDD, usingIndex, spatialPredicate)
	```

=== "Java"

	```java
	objectRDD.spatialPartitioning(joinQueryPartitioningType)
	queryWindowRDD.spatialPartitioning(objectRDD.getPartitioner)

	boolean buildOnSpatialPartitionedRDD = true // Set to TRUE only if run join query
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

	build_on_spatial_partitioned_rdd = True ## Set to TRUE only if run join query
	using_index = True
	query_window_rdd.buildIndex(IndexType.QUADTREE, build_on_spatial_partitioned_rdd)

	result = JoinQuery.SpatialJoinQueryFlat(object_rdd, query_window_rdd, using_index, True)
	```

The index should be built on either one of two SpatialRDDs. In general, you should build it on the larger SpatialRDD.

### Output format

=== "Scala/Java"

	The output format of the spatial join query is a PairRDD. In this PairRDD, each object is a pair of two geometries. The left one is the geometry from objectRDD and the right one is the geometry from the queryWindowRDD.

	```
	Point,Polygon
	Point,Polygon
	Point,Polygon
	Polygon,Polygon
	LineString,LineString
	Polygon,LineString
	...
	```

	Each object on the left is covered/intersected by the object on the right.

=== "Python"

	Result for this query is RDD which holds two GeoData objects within list of lists.
	Example:
	```python
	result.collect()
	```

	```
	[[GeoData, GeoData], [GeoData, GeoData] ...]
	```

	It is possible to do some RDD operation on result data ex. Getting polygon centroid.
	```python
	result.map(lambda x: x[0].geom.centroid).collect()
	```

!!!note
    Sedona Python users: Please use JoinQueryRaw from the same module for methods

    - spatialJoin

    - DistanceJoinQueryFlat

    - SpatialJoinQueryFlat

    For better performance while converting to dataframe with adapter.
    That approach allows to avoid costly serialization between Python
    and jvm and in result operating on python object instead of native geometries.

    Example:
    ```python
    from sedona.spark import CircleRDD
    from sedona.spark import GridType
    from sedona.spark import JoinQueryRaw
    from sedona.spark import StructuredAdapter

    object_rdd.analyze()

    circle_rdd = CircleRDD(object_rdd, 0.1)  ## Create a CircleRDD using the given distance
    circle_rdd.analyze()

    circle_rdd.spatialPartitioning(GridType.KDBTREE)
    spatial_rdd.spatialPartitioning(circle_rdd.getPartitioner())

    consider_boundary_intersection = False  ## Only return gemeotries fully covered by each query window in queryWindowRDD
    using_index = False

    result = JoinQueryRaw.DistanceJoinQueryFlat(
        spatial_rdd, circle_rdd, using_index, consider_boundary_intersection
    )

    gdf = StructuredAdapter.toDf(
        result, ["left_col1", ..., "lefcoln"], ["rightcol1", ..., "rightcol2"], spark
    )
    ```

## Write a Distance Join Query

!!!warning
	RDD distance joins are only reliable for points. For other geometry types, please use Spatial SQL.

A distance join query takes as input two Spatial RDD A and B and a distance. For each geometry in A, finds the geometries (from B) are within the given distance to it. A and B can be any geometry type and are not necessary to have the same geometry type. The unit of the distance is explained [here](#transform-the-coordinate-reference-system).

If you don't want to transform your data and are ok with sacrificing the query accuracy, you can use an approximate degree value for distance. Please use [this calculator](https://lucidar.me/en/online-unit-converter-length-to-angle/convert-degrees-to-meters/#online-converter).

Assume you now have two SpatialRDDs (typed or generic). You can use the following code to issue a Distance Join Query on them.

=== "Scala"

	```scala
	objectRddA.analyze()

	val circleRDD = new CircleRDD(objectRddA, 0.1) // Create a CircleRDD using the given distance

	circleRDD.spatialPartitioning(GridType.KDBTREE)
	objectRddB.spatialPartitioning(circleRDD.getPartitioner)

	val spatialPredicate = SpatialPredicate.COVERED_BY // Only return gemeotries fully covered by each query window in queryWindowRDD
	val usingIndex = false

	val result = JoinQuery.DistanceJoinQueryFlat(objectRddB, circleRDD, usingIndex, spatialPredicate)
	```

=== "Java"

	```java
	objectRddA.analyze()

	CircleRDD circleRDD = new CircleRDD(objectRddA, 0.1) // Create a CircleRDD using the given distance

	circleRDD.spatialPartitioning(GridType.KDBTREE)
	objectRddB.spatialPartitioning(circleRDD.getPartitioner)

	SpatialPredicate spatialPredicate = SpatialPredicate.COVERED_BY // Only return gemeotries fully covered by each query window in queryWindowRDD
	boolean usingIndex = false

	JavaPairRDD result = JoinQuery.DistanceJoinQueryFlat(objectRddB, circleRDD, usingIndex, spatialPredicate)
	```

=== "Python"

	```python
	from sedona.spark import CircleRDD
	from sedona.spark import GridType
	from sedona.spark import JoinQuery

	object_rdd.analyze()

	circle_rdd = CircleRDD(object_rdd, 0.1) ## Create a CircleRDD using the given distance
	circle_rdd.analyze()

	circle_rdd.spatialPartitioning(GridType.KDBTREE)
	spatial_rdd.spatialPartitioning(circle_rdd.getPartitioner())

	consider_boundary_intersection = False ## Only return gemeotries fully covered by each query window in queryWindowRDD
	using_index = False

	result = JoinQuery.DistanceJoinQueryFlat(spatial_rdd, circle_rdd, using_index, consider_boundary_intersection)
	```

Distance join can only accept `COVERED_BY` and `INTERSECTS` as spatial predicates. The rest part of the join query is same as the spatial join query.

The details of spatial partitioning in join query is [here](#use-spatial-partitioning).

The details of using spatial indexes in join query is [here](#use-spatial-indexes_2).

The output format of the distance join query is [here](#output-format_2).

!!!note
	Distance join query is equal to the following query in Spatial SQL:
	```sql
	SELECT superhero.name
	FROM city, superhero
	WHERE ST_Distance(city.geom, superhero.geom) <= 10;
	```
	Find the superheroes within 10 miles of each city

## Save to permanent storage

You can always save an SpatialRDD back to some permanent storage such as HDFS and Amazon S3.

### Save an SpatialRDD (not indexed)

Use the following code to save an SpatialRDD as a distributed object file:

=== "Scala/Java"

	```scala
	objectRDD.rawSpatialRDD.saveAsObjectFile("hdfs://PATH")
	```

=== "Python"

	```python
	object_rdd.rawJvmSpatialRDD.saveAsObjectFile("hdfs://PATH")
	```

!!!note
	Each object in a distributed object file is a byte array (not human-readable). This byte array is the serialized format of a Geometry or a SpatialIndex.

### Save an SpatialRDD (indexed)

Indexed typed SpatialRDD and generic SpatialRDD can be saved to permanent storage. However, the indexed SpatialRDD has to be stored as a distributed object file.

Use the following code to save an SpatialRDD as a distributed object file:

```
objectRDD.indexedRawRDD.saveAsObjectFile("hdfs://PATH")
```

### Save an SpatialRDD (spatialPartitioned W/O indexed)

A spatial partitioned RDD can be saved to permanent storage but Spark is not able to maintain the same RDD partition Id of the original RDD. This will lead to wrong join query results. We are working on some solutions. Stay tuned!

### Reload a saved SpatialRDD

You can easily reload an SpatialRDD that has been saved to ==a distributed object file==. Use the following code to reload the SpatialRDD:

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

Use the following code to reload the indexed SpatialRDD:

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
