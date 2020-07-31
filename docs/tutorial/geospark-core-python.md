# Spatial RDD Applications in Python

## Introduction
<div style="text-align: justify">
GeoSpark provides a Python wrapper on GeoSpark core Java/Scala library.
GeoSpark SpatialRDDs (and other classes when it was necessary) have implemented meta classes which allow 
to use overloaded functions, methods and constructors to be the most similar to Java/Scala API as possible. 
</div>

GeoSpark-core provides five special SpatialRDDs:

<li> PointRDD </li>
<li> PolygonRDD </li>
<li> LineStringRDD </li>
<li> CircleRDD </li>
<li> RectangleRDD </li>
<div style="text-align: justify">
<p>
All of them can be imported from <b> geospark.core.SpatialRDD </b> module

<b> geospark </b> has written serializers which convert GeoSpark SpatialRDD to Python objects.
Converting will produce GeoData objects which have 2 attributes:
</p>
</div>
<li> geom: shapely.geometry.BaseGeometry </li>
<li> userData: str </li>

geom attribute holds geometry representation as shapely objects. 
userData is string representation of other attributes separated by "\t"
</br>

GeoData has one method to get user data.
<li> getUserData() -> str </li>

## Installing the package

GeoSpark extends pyspark functions which depend on Python packages and Scala libraries. To see all dependencies
please look at the dependencies section.
https://pypi.org/project/pyspark/.

This package needs 2 jar files to work properly:

- geospark.jar
- geo_wrapper.jar

!!!note
    To enable GeoSpark Core functionality without GeoSparkSQL there is no need to copy jar files from geospark/jars
    location. You can use jar files from Maven repositories. Since GeoSpark 1.3.0 it is possible also to use maven 
    jars for GeoSparkSQL instead of geospark/jars/../geospark-sql jars files.


This package automatically copies the newest GeoSpark jar files using upload_jars function, please follow the example below.

<li> upload_jars </li>

```python

from pyspark.sql import SparkSession

from geospark.register import upload_jars
from geospark.register import GeoSparkRegistrator

upload_jars()

spark = SparkSession.builder.\
      getOrCreate()

GeoSparkRegistrator.registerAll(spark)

```

Function

```python

upload_jars()


```

uses findspark Python package to upload jar files to executor and nodes. To avoid copying all the time, jar files can be put in directory SPARK_HOME/jars or any other path specified in Spark config files.


### Installing from PyPi repositories

Please use command below
 
```bash
pip install geospark
```

### Installing from wheel file


```bash

pipenv run python -m pip install dist/geospark-1.3.1-py3-none-any.whl

```

or

```bash

pip install dist/geospark-1.3.1-py3-none-any.whl


```

### Installing from source


```bash

python3 setup.py install

```


## GeoSpark Serializers
GeoSpark has a suite of well-written geometry and index serializers. Forgetting to enable these serializers will lead to high memory consumption.

```python
conf.set("spark.serializer", KryoSerializer.getName)
conf.set("spark.kryo.registrator", GeoSparkKryoRegistrator.getName)
```

## Create a SpatialRDD

### Create a typed SpatialRDD
GeoSpark-core provides three special SpatialRDDs:
<li> PointRDD </li>
<li> PolygonRDD </li> 
<li> LineStringRDD </li>
<li> CircleRDD </li>
<li> RectangleRDD </li>
<br>

They can be loaded from CSV, TSV, WKT, WKB, Shapefiles, GeoJSON formats.
To pass the format to SpatialRDD constructor please use <b> FileDataSplitter </b> enumeration. 

geospark SpatialRDDs (and other classes when it was necessary) have implemented meta classes which allow 
to use overloaded functions how Scala/Java GeoSpark API allows. ex. 


```python
from pyspark import StorageLevel
from geospark.core.SpatialRDD import PointRDD
from geospark.core.enums import FileDataSplitter

input_location = "checkin.csv"
offset = 0  # The point long/lat starts from Column 0
splitter = FileDataSplitter.CSV # FileDataSplitter enumeration
carry_other_attributes = True  # Carry Column 2 (hotel, gas, bar...)
level = StorageLevel.MEMORY_ONLY # Storage level from pyspark
s_epsg = "epsg:4326" # Source epsg code
t_epsg = "epsg:5070" # target epsg code

point_rdd = PointRDD(sc, input_location, offset, splitter, carry_other_attributes)

point_rdd = PointRDD(sc, input_location, splitter, carry_other_attributes, level, s_epsg, t_epsg)

point_rdd = PointRDD(
    sparkContext=sc,
    InputLocation=input_location,
    Offset=offset,
    splitter=splitter,
    carryInputData=carry_other_attributes
)
```


#### From SparkSQL DataFrame
To create spatialRDD from other formats you can use adapter between Spark DataFrame and SpatialRDD

<li> Load data in GeoSparkSQL. </li>

```python
csv_point_input_location= "/tests/resources/county_small.tsv"

df = spark.read.\
    format("csv").\
    option("delimiter", "\t").\
    option("header", "false").\
    load(csv_point_input_location)

df.createOrReplaceTempView("counties")

```

<li> Create a Geometry type column in GeoSparkSQL </li>

```python
spatial_df = spark.sql(
    """
        SELECT ST_GeomFromWKT(_c0) as geom, _c6 as county_name
        FROM counties
    """
)
spatial_df.printSchema()
```

```
root
 |-- geom: geometry (nullable = false)
 |-- county_name: string (nullable = true)
```

<li> Use GeoSparkSQL DataFrame-RDD Adapter to convert a DataFrame to an SpatialRDD </li>
Note that, you have to name your column geometry

```python
from geospark.utils.adapter import Adapter

spatial_rdd = Adapter.toSpatialRdd(spatial_df)
spatial_rdd.analyze()

spatial_rdd.boundaryEnvelope
```

```
<geospark.core.geom_types.Envelope object at 0x7f1e5f29fe10>
```

or pass Geometry column name as a second argument

```python
spatial_rdd = Adapter.toSpatialRdd(spatial_df, "geom")
```

For WKT/WKB/GeoJSON data, please use ==ST_GeomFromWKT / ST_GeomFromWKB / ST_GeomFromGeoJSON== instead.
    
## Read other attributes in an SpatialRDD

Each SpatialRDD can carry non-spatial attributes such as price, age and name as long as the user sets ==carryOtherAttributes== as [TRUE](#create-a-spatialrdd).

The other attributes are combined together to a string and stored in ==UserData== field of each geometry.

To retrieve the UserData field, use the following code:
```python
rdd_with_other_attributes = object_rdd.rawSpatialRDD.map(lambda x: x.getUserData())
``` 

## Write a Spatial Range Query

```python
from geospark.core.geom.envelope import Envelope
from geospark.core.spatialOperator import RangeQuery

range_query_window = Envelope(-90.01, -80.01, 30.01, 40.01)
consider_boundary_intersection = False  ## Only return gemeotries fully covered by the window
using_index = False
query_result = RangeQuery.SpatialRangeQuery(spatial_rdd, range_query_window, consider_boundary_intersection, using_index)
```

### Range query window

Besides the rectangle (Envelope) type range query window, GeoSpark range query window can be 
<li> Point </li> 
<li> Polygon </li>
<li> LineString </li>
</br>

The code to create a point is as follows:
To create shapely geometries please follow official shapely <a href=""> documentation </a>  



### Use spatial indexes

GeoSpark provides two types of spatial indexes,
<li> Quad-Tree </li>
<li> R-Tree </li>
Once you specify an index type, 
GeoSpark will build a local tree index on each of the SpatialRDD partition.

To utilize a spatial index in a spatial range query, use the following code:

```python
from geospark.core.geom.envelope import Envelope
from geospark.core.enums import IndexType
from geospark.core.spatialOperator import RangeQuery

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

### Output format

The output format of the spatial range query is another RDD which consists of GeoData objects.

SpatialRangeQuery result can be used as RDD with map or other spark RDD funtions. Also it can be used as 
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

A spatial K Nearnest Neighbor query takes as input a K, a query point and an SpatialRDD and finds the K geometries in the RDD which are the closest to he query point.

Assume you now have an SpatialRDD (typed or generic). You can use the following code to issue an Spatial KNN Query on it.

```python
from geospark.core.spatialOperator import KNNQuery
from shapely.geometry import Point

point = Point(-84.01, 34.01)
k = 1000 ## K Nearest Neighbors
using_index = False
result = KNNQuery.SpatialKnnQuery(object_rdd, point, k, using_index)
```

### Query center geometry

Besides the Point type, GeoSpark KNN query center can be 
<li> Polygon </li>
<li> LineString </li>

To create Polygon or Linestring object please follow Shapely official <a href="https://shapely.readthedocs.io/en/stable/manual.html"> documentation </a>

### Use spatial indexes

To utilize a spatial index in a spatial KNN query, use the following code:

```python
from geospark.core.spatialOperator import KNNQuery
from geospark.core.enums import IndexType
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

The output format of the spatial KNN query is a list of GeoData objects. 
The list has K GeoData objects.

Example:
```python
>> result

[GeoData, GeoData, GeoData, GeoData, GeoData]
```


## Write a Spatial Join Query

A spatial join query takes as input two Spatial RDD A and B. For each geometry in A, finds the geometries (from B) covered/intersected by it. A and B can be any geometry type and are not necessary to have the same geometry type.

Assume you now have two SpatialRDDs (typed or generic). You can use the following code to issue an Spatial Join Query on them.

```python
from geospark.core.enums import GridType
from geospark.core.spatialOperator import JoinQuery

consider_boundary_intersection = False ## Only return geometries fully covered by each query window in queryWindowRDD
using_index = False

object_rdd.analyze()

object_rdd.spatialPartitioning(GridType.KDBTREE)
query_window_rdd.spatialPartitioning(object_rdd.getPartitioner())

result = JoinQuery.SpatialJoinQuery(object_rdd, query_window_rdd, using_index, consider_boundary_intersection)
```

Result of SpatialJoinQuery is RDD which consists of GeoData instance and list of GeoData instances which spatially intersects or 
are covered by GeoData. 

```python
result.collect())
```

```
[
    [GeoData, [GeoData, GeoData, GeoData, GeoData]],
    [GeoData, [GeoData, GeoData, GeoData]],
    [GeoData, [GeoData]],
    [GeoData, [GeoData, GeoData]],
    ...
    [GeoData, [GeoData, GeoData]]
]

```

### Use spatial partitioning

GeoSpark spatial partitioning method can significantly speed up the join query. Three spatial partitioning methods are available: KDB-Tree, Quad-Tree and R-Tree. Two SpatialRDD must be partitioned by the same way.

If you first partition SpatialRDD A, then you must use the partitioner of A to partition B.

```python
object_rdd.spatialPartitioning(GridType.KDBTREE)
query_window_rdd.spatialPartitioning(object_rdd.getPartitioner())
```

Or 

```python
query_window_rdd.spatialPartitioning(GridType.KDBTREE)
object_rdd.spatialPartitioning(query_window_rdd.getPartitioner())
```


### Use spatial indexes

To utilize a spatial index in a spatial join query, use the following code:

```python
from geospark.core.enums import GridType
from geospark.core.enums import IndexType
from geospark.core.spatialOperator import JoinQuery

object_rdd.spatialPartitioning(GridType.KDBTREE)
query_window_rdd.spatialPartitioning(object_rdd.getPartitioner())

build_on_spatial_partitioned_rdd = True ## Set to TRUE only if run join query
using_index = True
query_window_rdd.buildIndex(IndexType.QUADTREE, build_on_spatial_partitioned_rdd)

result = JoinQuery.SpatialJoinQueryFlat(object_rdd, query_window_rdd, using_index, True)
```

The index should be built on either one of two SpatialRDDs. In general, you should build it on the larger SpatialRDD.

### Output format

The output format of the spatial join query is a PairRDD. In this PairRDD, each object is a pair of two GeoData objects.
The left one is the GeoData from object_rdd and the right one is the GeoData from the query_window_rdd.

```
Point,Polygon
Point,Polygon
Point,Polygon
Polygon,Polygon
LineString,LineString
Polygon,LineString
...
```

example 
```python
result.collect()
```

```
[
 [GeoData, GeoData],
 [GeoData, GeoData],
 [GeoData, GeoData],
 [GeoData, GeoData],
 ...
 [GeoData, GeoData],
 [GeoData, GeoData]
]
```

Each object on the left is covered/intersected by the object on the right.

## Write a Distance Join Query

A distance join query takes two spatial RDD assuming that we have two SpatialRDD's:
<li> object_rdd </li>
<li> spatial_rdd </li>

And finds the geometries (from spatial_rdd) are within given distance to it. spatial_rdd and object_rdd
can be any geometry type (point, line, polygon) and are not necessary to have the same geometry type
 
You can use the following code to issue an Distance Join Query on them.

```python
from geospark.core.SpatialRDD import CircleRDD
from geospark.core.enums import GridType
from geospark.core.spatialOperator import JoinQuery

object_rdd.analyze()

circle_rdd = CircleRDD(object_rdd, 0.1) ## Create a CircleRDD using the given distance
circle_rdd.analyze()

circle_rdd.spatialPartitioning(GridType.KDBTREE)
spatial_rdd.spatialPartitioning(circle_rdd.getPartitioner())

consider_boundary_intersection = False ## Only return gemeotries fully covered by each query window in queryWindowRDD
using_index = False

result = JoinQuery.DistanceJoinQueryFlat(spatial_rdd, circle_rdd, using_index, consider_boundary_intersection)
```
### Output format

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

```
[
 <shapely.geometry.point.Point at 0x7efee2d28128>,
 <shapely.geometry.point.Point at 0x7efee2d280b8>,
 <shapely.geometry.point.Point at 0x7efee2d28fd0>,
 <shapely.geometry.point.Point at 0x7efee2d28080>,
 ...
]
```
    
## Save to permanent storage

You can always save an SpatialRDD back to some permanent storage such as HDFS and Amazon S3. You can save distributed SpatialRDD to WKT, GeoJSON and object files.

!!!note
    Non-spatial attributes such as price, age and name will also be stored to permanent storage.

### Save an SpatialRDD (not indexed)

Typed SpatialRDD and generic SpatialRDD can be saved to permanent storage.

#### Save to distributed WKT text file

Use the following code to save an SpatialRDD as a distributed WKT text file:

```python
object_rdd.rawSpatialRDD.saveAsTextFile("hdfs://PATH")
object_rdd.saveAsWKT("hdfs://PATH")
```

#### Save to distributed WKB text file

Use the following code to save an SpatialRDD as a distributed WKB text file:

```python
object_rdd.saveAsWKB("hdfs://PATH")
```

#### Save to distributed GeoJSON text file

Use the following code to save an SpatialRDD as a distributed GeoJSON text file:

```python
object_rdd.saveAsGeoJSON("hdfs://PATH")
```


#### Save to distributed object file

Use the following code to save an SpatialRDD as a distributed object file:

```python
object_rdd.rawJvmSpatialRDD.saveAsObjectFile("hdfs://PATH")
```

!!!note
    Each object in a distributed object file is a byte array (not human-readable). This byte array is the serialized format of a Geometry or a SpatialIndex.

### Save an SpatialRDD (indexed)

Indexed typed SpatialRDD and generic SpatialRDD can be saved to permanent storage. However, the indexed SpatialRDD has to be stored as a distributed object file.

#### Save to distributed object file

Use the following code to save an SpatialRDD as a distributed object file:

```python
object_rdd.indexedRawRDD.saveAsObjectFile("hdfs://PATH")
```

### Save an SpatialRDD (spatialPartitioned W/O indexed)

A spatial partitioned RDD can be saved to permanent storage but Spark is not able to maintain the same RDD partition Id of the original RDD. This will lead to wrong join query results. We are working on some solutions. Stay tuned!

### Reload a saved SpatialRDD

You can easily reload an SpatialRDD that has been saved to ==a distributed object file==.

#### Load to a typed SpatialRDD

Use the following code to reload the PointRDD/PolygonRDD/LineStringRDD:

```python
from geospark.core.formatMapper.disc_utils import load_spatial_rdd_from_disc, GeoType

polygon_rdd = load_spatial_rdd_from_disc(sc, "hdfs://PATH", GeoType.POLYGON)
point_rdd = load_spatial_rdd_from_disc(sc, "hdfs://PATH", GeoType.POINT)
linestring_rdd = load_spatial_rdd_from_disc(sc, "hdfs://PATH", GeoType.LINESTRING)
```

#### Load to a generic SpatialRDD

Use the following code to reload the SpatialRDD:

```python
saved_rdd = load_spatial_rdd_from_disc(sc, "hdfs://PATH", GeoType.GEOMETRY)
```

Use the following code to reload the indexed SpatialRDD:
```python
saved_rdd = SpatialRDD()
saved_rdd.indexedRawRDD = load_spatial_index_rdd_from_disc(sc, "hdfs://PATH")
```

## Read from other Geometry files

All below methods will return SpatialRDD object which can be used with Spatial functions such as Spatial Join etc.

### Read from WKT file
```python
from geospark.core.formatMapper import WktReader

WktReader.readToGeometryRDD(sc, wkt_geometries_location, 0, True, False)
```
```
<geospark.core.SpatialRDD.spatial_rdd.SpatialRDD at 0x7f8fd2fbf250>
```

### Read from WKB file
```python
from geospark.core.formatMapper import WkbReader

WkbReader.readToGeometryRDD(sc, wkb_geometries_location, 0, True, False)
```
```
<geospark.core.SpatialRDD.spatial_rdd.SpatialRDD at 0x7f8fd2eece50>
```
### Read from GeoJson file

```python
from geospark.core.formatMapper import GeoJsonReader

GeoJsonReader.readToGeometryRDD(sc, geo_json_file_location)
```
```
<geospark.core.SpatialRDD.spatial_rdd.SpatialRDD at 0x7f8fd2eecb90>
```
### Read from Shapefile

```python
from geospark.core.formatMapper.shapefileParser import ShapefileReader

ShapefileReader.readToGeometryRDD(sc, shape_file_location)
```
```
<geospark.core.SpatialRDD.spatial_rdd.SpatialRDD at 0x7f8fd2ee0710>
```

## Supported versions

Currently this python wrapper supports the following Spark, GeoSpark and Python versions

### Apache Spark

<li> 2.2 </li>
<li> 2.3 </li>
<li> 2.4 </li>


### GeoSpark

<li> 1.3.1 </li>
<li> 1.2.0 </li>

### Python

<li> 3.6 </li>
<li> 3.7 </li>

!!!note
    Other versions may also work (or partially) but were not tested yet
