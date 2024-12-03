## Sedona 1.7.0

Sedona 1.7.0 is compiled against Spark 3.3 / Spark 3.4 / Spark 3.5, Flink 1.19, Snowflake 7+, Java 8.

This release is a major release that includes new features, improvements, bug fixes, API breaking changes, and behavior changes.

### New Contributors

* @mvaaltola made their first contribution in https://github.com/apache/sedona/pull/1574
* @emmanuel-ferdman made their first contribution in https://github.com/apache/sedona/pull/1658
* @MohammadLotfiA made their first contribution in https://github.com/apache/sedona/pull/1659
* @golfalot made their first contribution in https://github.com/apache/sedona/pull/1673
* @AmirTallap made their first contribution in https://github.com/apache/sedona/pull/1675
* @freamdx made their first contribution in https://github.com/apache/sedona/pull/1704

### Highlights

* [X] Add a new join algorithm for distributed K Nearest Neighbor Join and a corresponding ST_KNN function
* [X] Add new spatial statistics algorithms DBSCAN, Local Outlier Factor, and Getis Ord Hot Spot Analysis
* [X] Add new DataFrame based readers for Shapefile, and GeoPackage
* [X] Add 10 new ST functions

### API breaking changes

* [X] The support of Spark 3.0, 3.1, 3.2 is dropped. Sedona is now only compatible with Spark 3.3, 3.4, and 3.5.
* [X] Rasterio is no longer a mandatory dependency. You can still use Sedona Raster without rasterio. If you need to write rasterio UDF in Sedona, you can install it separately.

### Behavior changes

* [X] JTS version is upgraded to 1.20.0. This may cause some behavior changes in ST functions that rely on JTS.
* [X] ST_Length, ST_Length2D and ST_LengthSpheroid now only return the length for line objects. It now returns 0 for polygon objects.
* [X] ST_Perimeter now only returns the perimeter for polygon objects. It now returns 0 for line objects.

### Bug

<ul>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-650'>SEDONA-650</a>] -         Fiona-Geopandas Compatibility Issue in Python 3.8
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-665'>SEDONA-665</a>] -         Docker build failed at ubuntu 22 with rasterio 1.4.0+
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-669'>SEDONA-669</a>] -         GeoParquet format should handle timestamp_ntz columns properly
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-670'>SEDONA-670</a>] -         GeoJSON reader does not work properly on DBR
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-672'>SEDONA-672</a>] -         Bug fix for ST_LengthSpheroid
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-673'>SEDONA-673</a>] -         Cannot load GeoParquet without bbox metadata when spatial filter is applied
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-677'>SEDONA-677</a>] -         Kryo deserialization for null envelopes results in unit envelopes
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-682'>SEDONA-682</a>] -         Sedona Spark 3.3 does not compile on Scala 2.13
</li>
</ul>

### New Feature

<ul>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-646'>SEDONA-646</a>] -         Shapefile data source for DataFrame API
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-647'>SEDONA-647</a>] -         Add ST_RemoveRepeatedPoints
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-648'>SEDONA-648</a>] -         Implement Distributed K Nearest Neighbor Join
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-652'>SEDONA-652</a>] -         Add ST_MakeEnvelope
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-654'>SEDONA-654</a>] -         Add ST_RotateY
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-655'>SEDONA-655</a>] -         DBSCAN
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-656'>SEDONA-656</a>] -         Add ST_Project
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-658'>SEDONA-658</a>] -         Add ST_Simplify
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-659'>SEDONA-659</a>] -         Upgrade jts version to 1.20.0
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-661'>SEDONA-661</a>] -         Local Outlier Factor
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-664'>SEDONA-664</a>] -         Add native GeoPackage reader
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-666'>SEDONA-666</a>] -         Add ST_Scale and ST_ScaleGeom
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-667'>SEDONA-667</a>] -         Getis Ord G Local
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-671'>SEDONA-671</a>] -         Spider random spatial data generator
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-675'>SEDONA-675</a>] -         Add ST_InterpolatePoint
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-676'>SEDONA-676</a>] -         Add ST_Perimeter
</li>
</ul>

### Improvement

<ul>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-636'>SEDONA-636</a>] -         datatype geometry is not supported when &#39;create table xxx (geom geometry)
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-640'>SEDONA-640</a>] -         Refactor support for multiple spark versions in the build
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-642'>SEDONA-642</a>] -         R – Adapt R package for split version of jars
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-644'>SEDONA-644</a>] -         R – Update for SedonaContext
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-649'>SEDONA-649</a>] -         Fix spelling in Java files
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-653'>SEDONA-653</a>] -         Add lenient mode for RS_Clip
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-663'>SEDONA-663</a>] -         Support spark connect in dataframe api
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-678'>SEDONA-678</a>] -         Fix ST_Length and ST_Length2D behavior
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-679'>SEDONA-679</a>] -         Fix ST_LengthSpheroid behavior
</li>
</ul>

### Task

<ul>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-651'>SEDONA-651</a>] -         Add spark prefix to all sedona spark config
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-662'>SEDONA-662</a>] -         Clean Up Dead Code from DBSCAN
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-668'>SEDONA-668</a>] -         Drop the support of Spark 3.0, 3.1, 3.2
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-674'>SEDONA-674</a>] -         Make the rasterio binding for sedona-python work with GDAL 3.10
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-680'>SEDONA-680</a>] -         Remove rasterio from mandatory dependency
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-681'>SEDONA-681</a>] -         Bump GeoTools version from 28.2 to 28.5
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-683'>SEDONA-683</a>] -         Exclude some repetitive dependencies
</li>
</ul>

## Sedona 1.6.1

Sedona 1.6.1 is compiled against Spark 3.3 / Spark 3.4 / Spark 3.5, Flink 1.19, Snowflake 7+, Java 8.

This release is a maintenance release that includes bug fixes and minor improvements.

### New Contributors

* @zhangfengcdt made their first contribution in https://github.com/apache/sedona/pull/1431
* @james-willis made their first contribution in https://github.com/apache/sedona/pull/1453

### Highlights

* [X] Add native DataFrame based GeoJSON reader and writer
* [X] 48 new ST functions added
* [X] GeoParquet reader and writer supports GeoParquet 1.1.0 covering column
* [X] Improve the error handling of ST functions so that the error message includes the geometry that caused the error

### API breaking changes

* [X] The following raster functions now return struct type outputs instead of array types.
   * RS_Metadata
   * RS_SummaryStatsAll
   * RS_ZonalStatsAll
   * RS_GeoTransform

### Bug

<ul>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-560'>SEDONA-560</a>] -         Spatial join involving dataframe containing 0 partition throws exception
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-561'>SEDONA-561</a>] -         Failed to run examples in the core.showcase package
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-580'>SEDONA-580</a>] -         New instances of RasterUDT object is not equal to the RasterUDT case object
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-581'>SEDONA-581</a>] -         SedonaKepler fails to reload if a raster column exists
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-605'>SEDONA-605</a>] -         RS_AsRaster(useGeometryExtent=false) does not work with reference rasters with scaleX/Y &lt; 1
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-608'>SEDONA-608</a>] -         Fix ST_IsPolygonCW, ST_IsPolygonCCW, ST_ForcePolygonCW and ST_ForcePolygonCCW
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-609'>SEDONA-609</a>] -         Fix python 3.12 build issue caused by binary compatibility issues with numpy 2.0.0
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-611'>SEDONA-611</a>] -         Cannot write rasters to S3 on EMR
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-618'>SEDONA-618</a>] -         Maven build failed with javadoc classes and package list files missing
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-624'>SEDONA-624</a>] -         Distance join throws java.lang.reflect.InvocationTargetException when working with aggregation functions
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-626'>SEDONA-626</a>] -         SRID of geometries returned by many ST functions are incorrect
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-628'>SEDONA-628</a>] -         Python DataFrame Functions Cannot Be Imported As Documented
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-639'>SEDONA-639</a>] -         ST_Split may produce inaccurate results when splitting linestrings
</li>
</ul>

### New Feature

<ul>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-462'>SEDONA-462</a>] -         ST_IsValidDetail
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-486'>SEDONA-486</a>] -         Implement ST_MMin
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-487'>SEDONA-487</a>] -         Implement ST_MMax
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-562'>SEDONA-562</a>] -         Add native DataFrame based GeoJSON reader and writer
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-563'>SEDONA-563</a>] -         Add ST_GeomFromEWKB
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-564'>SEDONA-564</a>] -         Add ST_NumInteriorRing
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-565'>SEDONA-565</a>] -         Add ST_ForceRHR
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-566'>SEDONA-566</a>] -         Add ST_TriangulatePolygon
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-567'>SEDONA-567</a>] -         Add ST_M
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-569'>SEDONA-569</a>] -         Add ST_PointZM
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-570'>SEDONA-570</a>] -         Add ST_PointM
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-571'>SEDONA-571</a>] -         Add ST_MMin
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-572'>SEDONA-572</a>] -         Add ST_PointFromWKB
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-573'>SEDONA-573</a>] -         Add ST_HasM
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-574'>SEDONA-574</a>] -         Add ST_MMax
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-575'>SEDONA-575</a>] -         Add ST_LineFromWKB
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-576'>SEDONA-576</a>] -         Add ST_HasZ
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-577'>SEDONA-577</a>] -         Add ST_GeometryFromText
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-578'>SEDONA-578</a>] -         Add ST_Points
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-579'>SEDONA-579</a>] -         Add ST_AsHEXEWKB
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-582'>SEDONA-582</a>] -         Add ST_PointFromGeoHash
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-583'>SEDONA-583</a>] -         Add ST_Length2D
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-584'>SEDONA-584</a>] -         Add ST_Zmflag
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-585'>SEDONA-585</a>] -         Add ST_ForceCollection
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-586'>SEDONA-586</a>] -         Add ST_Force3DZ
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-587'>SEDONA-587</a>] -         Add ST_Force3DM
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-588'>SEDONA-588</a>] -         Add ST_Force4D
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-589'>SEDONA-589</a>] -         Add ST_LongestLine
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-590'>SEDONA-590</a>] -         Add ST_GeomColFromText
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-591'>SEDONA-591</a>] -         Add ST_MaxDistance
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-592'>SEDONA-592</a>] -         Add ST_MPointFromText
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-593'>SEDONA-593</a>] -         Add ST_Relate
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-594'>SEDONA-594</a>] -         Add ST_RelatedMatch
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-595'>SEDONA-595</a>] -         Add ST_LineStringFromWKB
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-596'>SEDONA-596</a>] -         Add ST_SimplifyVW
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-597'>SEDONA-597</a>] -         Add ST_SimplifyPolygonHull
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-598'>SEDONA-598</a>] -         Add ST_UnaryUnion
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-599'>SEDONA-599</a>] -         Add ST_MinimumClearance
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-600'>SEDONA-600</a>] -         Add ST_MinimumClearanceLine
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-601'>SEDONA-601</a>] -         Add ST_DelaunyTriangles
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-602'>SEDONA-602</a>] -         Add ST_LocateAlong
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-603'>SEDONA-603</a>] -         Add ST_MakePointM
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-604'>SEDONA-604</a>] -         Add ST_AddMeasure
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-606'>SEDONA-606</a>] -         Add ST_IsValidDetail
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-607'>SEDONA-607</a>] -         Include Geometry in ST Function Exceptions
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-610'>SEDONA-610</a>] -         Add ST_IsValidTrajectory
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-615'>SEDONA-615</a>] -         Add ST_MaximumInscribedCircle
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-617'>SEDONA-617</a>] -         Add ST_Rotate
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-625'>SEDONA-625</a>] -         Add ST_GeneratePoints
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-627'>SEDONA-627</a>] -         Writing covering column metadata to GeoParquet files
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-631'>SEDONA-631</a>] -         Add ST_Expand
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-643'>SEDONA-643</a>] -         Fix Flink constructor functions signatures
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-645'>SEDONA-645</a>] -         Add ST_RotateX
</li>
</ul>

### Improvement

<ul>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-558'>SEDONA-558</a>] -         Fix and improve SedonaPyDeck behavior
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-559'>SEDONA-559</a>] -         Make the flink example work
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-568'>SEDONA-568</a>] -         Refactor TestBaseScala to use method instead of a class-level variable for sparkSession
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-616'>SEDONA-616</a>] -         Apply spotless to snowflake module
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-620'>SEDONA-620</a>] -         Simplify Java if statements
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-621'>SEDONA-621</a>] -         Remove redundant call to `toString()`
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-622'>SEDONA-622</a>] -         Improve SedonaPyDeck behavior
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-623'>SEDONA-623</a>] -         Simplify Java `if` statements
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-629'>SEDONA-629</a>] -         Return Structs for RS_ Functions
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-632'>SEDONA-632</a>] -         Don&#39;t use a conventional output committer when writing raster files using df.write.format(&quot;raster&quot;)
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-633'>SEDONA-633</a>] -         Add tileWidth and tileHeight fields to the result of RS_Metadata
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-634'>SEDONA-634</a>] -         Support omitting tileWidth and tileHeight parameters when calling RS_Tile or RS_TileExplode on rasters with decent tiling scheme
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-635'>SEDONA-635</a>] -         Allow feature and feature collection format in ST_AsGeoJSON
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-637'>SEDONA-637</a>] -         Show spatial filters pushed to GeoParquet scans in the query plan
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-638'>SEDONA-638</a>] -         Send telemetry data asynchronously to avoid blocking the initialization of SedonaContext
</li>
</ul>

### Task

<ul>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-101'>SEDONA-101</a>] -         Add Scala Formatter to MVN
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-102'>SEDONA-102</a>] -         Java Code Formatting using formatter plugin
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-553'>SEDONA-553</a>] -         Update Sedona docker to use newer GeoPandas
</li>
</ul>

## Sedona 1.6.0

Sedona 1.6.0 is compiled against Spark 3.3 / Spark 3.4 / Spark 3.5, Flink 1.19, Snowflake 7+, Java 8.

### New Contributors

* @mpetazzoni made their first contribution in https://github.com/apache/sedona/pull/1216
* @sebdiem made their first contribution in https://github.com/apache/sedona/pull/1217
* @guilhem-dvr made their first contribution in https://github.com/apache/sedona/pull/1229
* @niklas-petersen made their first contribution in https://github.com/apache/sedona/pull/1252
* @mebrein made their first contribution in https://github.com/apache/sedona/pull/1334
* @docete made their first contribution in https://github.com/apache/sedona/pull/1409

### Highlights

* [X] Sedona is now compatible with Shapely 2.0 and GeoPandas 0.11.1+.
* [X] Sedona added enhanced support for geography data. This includes
    * ST_Buffer with spheroid distance
    * ST_BestSRID to find the best SRID for a geometry
    * ST_ShiftLongitude to shift the longitude of a geometry to mitigate the issue of crossing the date line
    * ST_CrossesDateLine to check if a geometry crosses the date line
    * ST_DWithin now supports spheroid distance
* [X] **Sedona Spark** Sedona Raster allows RS_ReropjectMatch to wrap the extent of one raster to another raster, similar to [RasterArray.reproject_match function in rioxarray](https://corteva.github.io/rioxarray/html/rioxarray.html#rioxarray.raster_array.RasterArray.reproject_match)
* [X] **Sedona Spark** Sedona Raster now supports Rasterio and NumPy UDF by `raster.as_numpy`, `raster.as_numpy_masked`, `raster.as_rasterio`. You can perform any native function from rasterio and numpy and run them in parallel. See the example below.

```python
from pyspark.sql.types import DoubleType

def mean_udf(raster):
	return float(raster.as_numpy().mean())

sedona.udf.register("mean_udf", mean_udf, DoubleType())
df_raster.withColumn("mean", expr("mean_udf(rast)")).show()
```

### Bug

<ul>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-532'>SEDONA-532</a>] - Sedona Spark SQL optimizer cannot optimize joins with complex conditions
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-543'>SEDONA-543</a>] - RS_Union_aggr gives referenceRaster is null error when run on cluster
</li>
</ul>

### New Feature

<ul>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-467'>SEDONA-467</a>] - Add optimized join support for ST_DWithin
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-468'>SEDONA-468</a>] - Add provision to use spheroid distance in ST_DWithin
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-475'>SEDONA-475</a>] - Add RS_NormalizeAll to normalize all bands of a raster
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-480'>SEDONA-480</a>] - Implement ST_S2ToGeom
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-481'>SEDONA-481</a>] - Implements ST_Snap
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-484'>SEDONA-484</a>] - Implement ST_IsPolygonCW
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-488'>SEDONA-488</a>] - ST_Buffer with spheroid distance
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-498'>SEDONA-498</a>] - Add ST_BestSRID
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-499'>SEDONA-499</a>] - Add Spheroidal ST_Buffer
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-504'>SEDONA-504</a>] - Add ST_ShiftLongitude
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-508'>SEDONA-508</a>] - Add ST_CrossesDateLine
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-509'>SEDONA-509</a>] - Add Single Statistic RS_SummaryStats
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-514'>SEDONA-514</a>] - Add RS_SetPixelType
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-516'>SEDONA-516</a>] - Add RS_Interpolate
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-517'>SEDONA-517</a>] - Add RS_MakeRaster for constructing a new raster using given array data as band data
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-518'>SEDONA-518</a>] - Add RS_ReprojectMatch for wrapping the extent of one raster to another raster
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-522'>SEDONA-522</a>] - Add ST_Union with array of Geometry as input
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-533'>SEDONA-533</a>] - Implement ST_Polygonize
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-539'>SEDONA-539</a>] - Support Snowflake geography type
</li>
</ul>

### Improvement

<ul>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-483'>SEDONA-483</a>] - Implements ST_IsPolygonCCW
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-493'>SEDONA-493</a>] - Update default behavior of RS_NormalizeAll
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-503'>SEDONA-503</a>] - Support Shapely 2.0 in PySpark binding
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-521'>SEDONA-521</a>] - Change ST_H3ToGeom Behavior
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-549'>SEDONA-549</a>] - RS_Union_aggr should support combining all bands in multi-band rasters
</li>
</ul>

### Task

<ul>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-540'>SEDONA-540</a>] - Fix failed ST_Buffer and ST_Snap Snowflake tests
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-550'>SEDONA-550</a>] - Remove the version upper bound of Pandas, GeoPandas
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-557'>SEDONA-557</a>] - Bump Flink from 1.14.x to 1.19.0
</ul>

## Sedona 1.5.3

Sedona 1.5.3 is compiled against Spark 3.3 / Spark 3.4 / Spark 3.5, Flink 1.12, Snowflake 7+, Java 8.

This release is a maintenance release that includes one bug fix on top of Sedona 1.5.2. No new features or major changes are added in this release.

### Bug

<ul>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-556'>SEDONA-556</a>] - Hidden requirement for geopandas in apache-sedona 1.5.2
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-555'>SEDONA-555</a>] - Snowflake Native App should not always create a new role
</li>
</ul>

## Sedona 1.5.2

Sedona 1.5.2 is compiled against Spark 3.3 / Spark 3.4 / Spark 3.5, Flink 1.12, Snowflake 7+, Java 8.

This release is a maintenance release that includes bug fixes and minor improvements. No new features or major changes are added in this release.

### New Contributors

* @mpetazzoni made their first contribution in https://github.com/apache/sedona/pull/1216
* @sebdiem made their first contribution in https://github.com/apache/sedona/pull/1217
* @guilhem-dvr made their first contribution in https://github.com/apache/sedona/pull/1229
* @niklas-petersen made their first contribution in https://github.com/apache/sedona/pull/1252
* @mebrein made their first contribution in https://github.com/apache/sedona/pull/1334

### Bug

<ul>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-470'>SEDONA-470</a>] - Cannot distinguish between missing or null crs from the result of geoparquet.metadata
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-471'>SEDONA-471</a>] - SedonaKepler cannot work with Uber H3 hex since 1.5.1
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-472'>SEDONA-472</a>] - Adapter API no longer works with unshaded jar
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-473'>SEDONA-473</a>] - cdm-core mistakenly becomes a compile dependency for sedona-spark-shaded
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-477'>SEDONA-477</a>] - Avoid producing rasters with images having non-zero origins
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-478'>SEDONA-478</a>] - Sedona 1.5.1 context initialization fails without GeoTools coverage
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-479'>SEDONA-479</a>] - Fix RS_Normalize: Incorrect behavior for double arrays
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-494'>SEDONA-494</a>] - Raster data source cannot write to HDFS
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-495'>SEDONA-495</a>] - Raster data source uses shared FileSystem connections which lead to race condition
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-497'>SEDONA-497</a>] - SpatialRDD read from multiple Shapefiles has incorrect fieldName property
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-500'>SEDONA-500</a>] - Cannot correctly read data from directories containing multiple shapefiles
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-501'>SEDONA-501</a>] - ST_Split maps to wrong Java-call
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-505'>SEDONA-505</a>] - Treat geometry with SRID=0 as if it was in EPSG:4326 in various raster functions
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-507'>SEDONA-507</a>] - RS_AsImage cannot visualize rasters with non-integral band data
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-510'>SEDONA-510</a>] - geometry columns with snake_case names in GeoParquet files cannot be recognized as geometry column
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-511'>SEDONA-511</a>] - geometry columns with snake_case names in GeoParquet files cannot be recognized as geometry column
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-519'>SEDONA-519</a>] - ST_SubDivide (Snowflake) fails even on documentation example
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-520'>SEDONA-520</a>] - Missing dependencies in Snowflake JAR
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-531'>SEDONA-531</a>] - RDD spatial join in Python throws Not available error
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-534'>SEDONA-534</a>] - Disable Python warning message of finding jars
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-545'>SEDONA-545</a>] - Sedona Python DataFrame API fail due to missing commas
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-548'>SEDONA-548</a>] - Fix Python Dataframe API Constructor registrations
</li>
</ul>

### Improvement

<ul>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-474'>SEDONA-474</a>] - Remove manipulation of warnings config
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-506'>SEDONA-506</a>] - Add lenient mode for RS_ZonalStats and RS_ZonalStatsAll
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-512'>SEDONA-512</a>] - Python serializer should report the object type in the error message
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-515'>SEDONA-515</a>] - Add handling for noDataValues in RS_Resample
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-529'>SEDONA-529</a>] - Add basic `EditorConfig` file
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-535'>SEDONA-535</a>] - Add the pull request labeler
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-536'>SEDONA-536</a>] - Add CODEOWNERS file
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-541'>SEDONA-541</a>] - Allow concurrent snowflake testers
</li>
</ul>

### Test

<ul>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-513'>SEDONA-513</a>] - Add pre-commit hook `mixed-line-ending`
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-523'>SEDONA-523</a>] - Add pre-commit hook `fix-byte-order-marker`
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-524'>SEDONA-524</a>] - Clean up the `pre-commit` config
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-525'>SEDONA-525</a>] - Add two more pre-commit hooks
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-528'>SEDONA-528</a>] - Add `pre-commit` hook `check-yaml`
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-530'>SEDONA-530</a>] - Add `pre-commit` hook `debug-statements`
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-537'>SEDONA-537</a>] - Add pre-commit hook `requirements-txt-fixer`
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-538'>SEDONA-538</a>] - Add four more pre-commit hooks
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-542'>SEDONA-542</a>] - Add `pre-commit` hook `check-executables-have-shebangs`
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-544'>SEDONA-544</a>] - Add `ruff-pre-commit` for `Python` linting
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-546'>SEDONA-546</a>] - Python linting enable rule `E712`
</li>
</ul>

### Task

<ul>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-469'>SEDONA-469</a>] - Update Sedona docker and binder to use 1.5.1
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-496'>SEDONA-496</a>] - Dependabot: reduce the open pull requests limit to 2
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-526'>SEDONA-526</a>] - Upgrade `actions/setup-java` to `v4`
</li>
</ul>

## Sedona 1.5.1

Sedona 1.5.1 is compiled against Spark 3.3 / Spark 3.4 / Spark 3.5, Flink 1.12, Snowflake 7+, Java 8.

### Highlights

* [X] **Sedona Snowflake** Add support for Snowflake
* [X] **Sedona Spark** Support Spark 3.5
* [X] **Sedona Spark** Support Snowflake 7+
* [X] **Sedona Spark** Added 20+ raster functions (or variants)
* [X] **Sedona Spark/Flink/Snowflake** Added 7 vector functions (or variants)
* [X] **Sedona Spark** GeoParquet reader and writer supports projjson in metadata
* [X] **Sedona Spark** GeoParquet reader and writer conform to GeoParquet spec 1.0.0 instead of 1.0.0-beta1
* [X] **Sedona Spark** Added a legacyMode in GeoParquet reader for 1.5.1+ users to read Parquet files written by Sedona 1.3.1 and earlier
* [X] **Sedona Spark** Fixed a bug in GeoParquet writer so 1.3.1 and earlier users can read Parquet files written by 1.5.1+

### Behavior change

* All raster functions that take a geometry will implicitly transform the CRS of the geometry if needed.
* The default CRS for these functions is 4326 for raster and geometry involved in raster functions, if not specified.
* KeplerGL and DeckGL become optional dependencies for Sedona Spark Python.

### New Contributors

* @hongbo-miao made their first contribution in https://github.com/apache/sedona/pull/1063
* @prantogg made their first contribution in https://github.com/apache/sedona/pull/1122
* @MyEnthusiastic made their first contribution in https://github.com/apache/sedona/pull/1130
* @duhaode520 made their first contribution in https://github.com/apache/sedona/pull/1193

### Bug

<ul>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-414'>SEDONA-414</a>] - ST_MakeLine in sedona-spark does not work with array inputs
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-417'>SEDONA-417</a>] - Fix SedonaUtils.display_image
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-419'>SEDONA-419</a>] - SedonaKepler and SedonaPyDeck should not be in `sedona.spark`
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-420'>SEDONA-420</a>] - Make SedonaKepler and SedonaPydeck optional dependencies
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-424'>SEDONA-424</a>] - Specify jt-jiffle as a provided dependency
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-426'>SEDONA-426</a>] - Change cloning of rasters to be able to include metadata.
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-440'>SEDONA-440</a>] - GeoParquet reader should support filter pushdown on nested fields
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-443'>SEDONA-443</a>] - Upload-artifact leads to 503 error
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-453'>SEDONA-453</a>] - Performance degrade when indexing points using Quadtree
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-456'>SEDONA-456</a>] - SedonaKepler cannot work with geopandas &gt;= 0.13.0 correctly
</li>
</ul>

### New Feature

<ul>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-369'>SEDONA-369</a>] - Add ST_DWITHIN
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-411'>SEDONA-411</a>] - Add RS_Rotation
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-413'>SEDONA-413</a>] - Add buffer parameters to ST_Buffer
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-415'>SEDONA-415</a>] - Add optional parameter to ST_Transform
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-421'>SEDONA-421</a>] - Add RS_Clip
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-422'>SEDONA-422</a>] - Add a feature in RS_SetBandNoDataValue and fix NoDataValue in RS_Clip
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-427'>SEDONA-427</a>] - Add RS_RasterToWorldCoord
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-428'>SEDONA-428</a>] - Add RS_ZonalStats &amp; RS_ZonalStatsAll
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-430'>SEDONA-430</a>] - geoparquet writer should have an option called `writeToCrs`
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-431'>SEDONA-431</a>] - Add RS_PixelAsPoints
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-432'>SEDONA-432</a>] - Add RS_PixelAsCentroids
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-433'>SEDONA-433</a>] - Improve RS_SummaryStats performance
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-435'>SEDONA-435</a>] - Add RS_PixelAsPolygons
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-438'>SEDONA-438</a>] - Add NetCDF reader to Sedona
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-439'>SEDONA-439</a>] - Add RS_Union_Aggr
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-441'>SEDONA-441</a>] - Implement ST_LineLocatePoint
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-449'>SEDONA-449</a>] - Add two raster column support to RS_MapAlgebra
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-455'>SEDONA-455</a>] - Add a new data source namely geoparquet.metadata
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-459'>SEDONA-459</a>] - Add Snowflake support
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-460'>SEDONA-460</a>] - RS_Tile and RS_TileExplode
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-461'>SEDONA-461</a>] - ST_IsValidReason
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-465'>SEDONA-465</a>] - Support reading legacy parquet files written by Apache Sedona &lt;= 1.3.1-incubating
</li>
</ul>

### Improvement

<ul>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-339'>SEDONA-339</a>] - Skip irrelevant GitHub actions
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-416'>SEDONA-416</a>] - importing SedonaContext, kepler.gl is not found.
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-429'>SEDONA-429</a>] - geoparquet reader/writer should print &quot;1.0.0&quot; in its version
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-434'>SEDONA-434</a>] - Improve reliability by resolve the nondeterministic of the order of the Map
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-436'>SEDONA-436</a>] - Fix RS_SetValues bug
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-437'>SEDONA-437</a>] - Add implicit CRS transformation
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-446'>SEDONA-446</a>] - Add floating point datatype support in RS_AsBase64
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-448'>SEDONA-448</a>] - RS_SetBandNoDataValue should have `replace` option
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-454'>SEDONA-454</a>] - Change the default value of sedona.global.indextype from quadtree to rtree
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-457'>SEDONA-457</a>] - Don&#39;t write GeometryUDT into org.apache.spark.sql.parquet.row.metadata when writing GeoParquet files
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-464'>SEDONA-464</a>] - ST_Valid should have integer flags
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-466'>SEDONA-466</a>] - RS_AsRaster does not use the weight and height of the raster in its parameters.
</li>
</ul>

### Test

<ul>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-410'>SEDONA-410</a>] - pre-commit: check that scripts with shebangs are executable
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-412'>SEDONA-412</a>] - pre-commit: add hook `end-of-file-fixer`
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-423'>SEDONA-423</a>] - pre-commit: apply hook `end-of-file-fixer` to more files
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-442'>SEDONA-442</a>] - pre-commit: add hook markdown-lint
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-444'>SEDONA-444</a>] - pre-commit: add hook to trim trailing whitespace
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-445'>SEDONA-445</a>] - pre-commit: apply hook end-of-file-fixer to more files
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-447'>SEDONA-447</a>] - pre-commit: apply end-of-file-fixer to more files
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-463'>SEDONA-463</a>] - Add a Makefile for convenience
</li>
</ul>

### Task

<ul>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-450'>SEDONA-450</a>] - Support Spark 3.5
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-458'>SEDONA-458</a>] - The docs should have examples for UDF
</li>
</ul>

## Sedona 1.5.0

Sedona 1.5.0 is compiled against Spark 3.3 / Spark 3.4 / Flink 1.12, Java 8.

### Highlights

**API breaking changes**:

* The following functions in Sedona requires the input data must be in longitude/latitude order otherwise they might throw errors. You can use `FlipCoordinates` to swap X and Y.
	* ST_Transform
	* ST_DistanceSphere
	* ST_DistanceSpheroid
	* ST_GeoHash
	* All ST_H3 functions
	* All ST_S2 functions
	* All RS constructors
	* All RS predicates
	* Spark RDD: CRStransform
* Rename `RS_Count` to `RS_CountValue`
* Drop `RS_HTML`
* Unshaded Sedona Spark code are all merged to a single jar `sedona-spark`

**New features**

* Add 18 more ST functions for vector data processing in Sedona Spark and Sedona Flink
* Add 36 more RS functions in Sedona Spark to support [comprehensive raster data ETL and analytics](../tutorial/raster.md)
	* You can now directly join vector and raster datasets together
	* Flexible map algebra equations: `SELECT RS_MapAlgebra(rast, 'D', 'out = (rast[3] - rast[0]) / (rast[3] + rast[0]);') as ndvi FROM raster_table`
* Add native support of [Uber H3 functions](../api/sql/Function.md#st_h3celldistance) in Sedona Spark and Sedona Flink.
* Add SedonaKepler and SedonaPyDeck for [interactive map visualization](../tutorial/sql.md#visualize-query-results) on Sedona Spark.

### Bug

<ul>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-318'>SEDONA-318</a>] - SerDe for RasterUDT performs poorly
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-319'>SEDONA-319</a>] - RS_AddBandFromArray does not always produce serializable rasters
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-322'>SEDONA-322</a>] - The &quot;Scala and Java build&quot; CI job occasionally fail
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-325'>SEDONA-325</a>] - RS_FromGeoTiff is leaking file descriptors
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-329'>SEDONA-329</a>] - Remove geometry_col parameter from SedonaKepler APIs
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-330'>SEDONA-330</a>] - Fix bugs in SedonaPyDeck
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-332'>SEDONA-332</a>] - RS_Value and RS_Values don&#39;t need to fetch all the pixel data
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-337'>SEDONA-337</a>] - Failure falling back to pure python implementation when geomserde_speedup is unavailable
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-338'>SEDONA-338</a>] - Refactor Raster construction in sedona to use AffineTransform instead of envelope
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-358'>SEDONA-358</a>] - Refactor Functions to remove geotools dependency for most vector functions
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-362'>SEDONA-362</a>] - RS_BandAsArray truncates the decimal part of float/double pixel values.
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-373'>SEDONA-373</a>] - Move RasterPredicates to correct raster package to prevent redundant imports
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-394'>SEDONA-394</a>] - fix RS_Band data type bug
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-401'>SEDONA-401</a>] - Handle null values in RS_AsMatrix
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-402'>SEDONA-402</a>] - Floor grid coordinates received from geotools
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-403'>SEDONA-403</a>] - Add Null tolerance to RS_AddBandFromArray
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-405'>SEDONA-405</a>] - Sedona driver Out of Memory on 1.4.1
</li>
</ul>

### New Feature

<ul>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-200'>SEDONA-200</a>] - Add ST_CoordDim to Sedona
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-213'>SEDONA-213</a>] - Add ST_BoundingDiagonal to Sedona
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-237'>SEDONA-237</a>] - Implement ST_Dimension
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-238'>SEDONA-238</a>] - Implement OGC GeometryType
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-293'>SEDONA-293</a>] - Implement ST_IsCollection
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-294'>SEDONA-294</a>] - Implement ST_Angle
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-295'>SEDONA-295</a>] - Implement ST_LineInterpolatePoint in Flink
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-296'>SEDONA-296</a>] - Implement ST_Multi in Sedona Flink
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-298'>SEDONA-298</a>] - Implement ST_ClosestPoint
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-299'>SEDONA-299</a>] - Implement ST_FrechetDistance
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-300'>SEDONA-300</a>] - Implement ST_HausdorffDistance
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-301'>SEDONA-301</a>] - Implement ST_Affine
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-303'>SEDONA-303</a>] - Port all Sedona Spark functions to Sedona Flink
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-310'>SEDONA-310</a>] - Add ST_Degrees to sedona
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-314'>SEDONA-314</a>] - Support Optimized join on ST_HausdorffDistance
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-315'>SEDONA-315</a>] - Support Optimized join on ST_FrechetDistance
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-321'>SEDONA-321</a>] - Implement RS_Intersects(raster, geom)
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-323'>SEDONA-323</a>] - Add wrapper for KeplerGl visualization in sedona
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-328'>SEDONA-328</a>] - Add wrapper for pydeck visualizations in sedona
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-331'>SEDONA-331</a>] - Add RS_Height and RS_Width
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-334'>SEDONA-334</a>] - Add ScaleX and ScaleY
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-335'>SEDONA-335</a>] - Add RS_PixelAsPoint
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-336'>SEDONA-336</a>] - Add RS_UpperLeftX and RS_UpperLeftY
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-340'>SEDONA-340</a>] - Add RS_ConvexHull
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-343'>SEDONA-343</a>] - Add raster predicates: Contains and Within
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-344'>SEDONA-344</a>] - Add RS_RasterToWorldCoordX, RS_RasterToWorldCoordY
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-346'>SEDONA-346</a>] - Add RS_WorldToRaster APIs
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-353'>SEDONA-353</a>] - Add RS_BandNoDataValue
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-354'>SEDONA-354</a>] - Add RS_SkewX and RS_SkewY
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-355'>SEDONA-355</a>] - Add RS_BandPixelType
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-357'>SEDONA-357</a>] - Implement ST_VoronoiPolygons
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-359'>SEDONA-359</a>] - Add RS_GeoReference
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-361'>SEDONA-361</a>] - Add RS_MapAlgebra for performing map algebra operations using simple expressions
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-363'>SEDONA-363</a>] - Add RS_PixelAsPolygon
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-364'>SEDONA-364</a>] - Add RS_MinConvexHull
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-366'>SEDONA-366</a>] - Add RS_Count
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-367'>SEDONA-367</a>] - Add RS_PixelAsCentroid
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-368'>SEDONA-368</a>] - Add RS_SummaryStats
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-371'>SEDONA-371</a>] - Add optimized join support for raster-vector and raster-raster(if any) joins
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-372'>SEDONA-372</a>] - Add RS_SetGeoReference
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-375'>SEDONA-375</a>] - Add RS_SetBandNoDataValue
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-376'>SEDONA-376</a>] - Add RS_SetValues
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-378'>SEDONA-378</a>] - Add RS_SetValue
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-379'>SEDONA-379</a>] - Add RS_AsBase64
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-383'>SEDONA-383</a>] - Add RS_Band
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-387'>SEDONA-387</a>] - Add RS_BandIsNoData
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-388'>SEDONA-388</a>] - Add RS_AsRaster
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-391'>SEDONA-391</a>] - Add RS_AsMatrix
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-393'>SEDONA-393</a>] - Add RS_AsPNG
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-395'>SEDONA-395</a>] - Add RS_AsImage
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-396'>SEDONA-396</a>] - Add RS_SetValues Geometry variant
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-398'>SEDONA-398</a>] - Add RS_AddBand
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-404'>SEDONA-404</a>] - Add RS_Resample
</li>
</ul>

### Improvement

<ul>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-39'>SEDONA-39</a>] - Fix the Lon/lat order issue in Sedona
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-114'>SEDONA-114</a>] - Add ST_MakeLine to Apache Sedona
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-142'>SEDONA-142</a>] - Add ST_Collect to Flink Catalog
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-311'>SEDONA-311</a>] - Refactor InferredExpression to handle functions with arbitrary arity
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-313'>SEDONA-313</a>] - Refactor ST_Affine to support signature like PostGIS
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-324'>SEDONA-324</a>] - R – Fix failing tests
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-326'>SEDONA-326</a>] - Improve raster band algebra functions for easier preprocessing of raster data
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-327'>SEDONA-327</a>] - Refactor InferredExpression to handle GridCoverage2D
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-333'>SEDONA-333</a>] - Support EWKT parser in ST_GeomFromWKT
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-347'>SEDONA-347</a>] - Centralize usages of transform()
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-350'>SEDONA-350</a>] - Refactor RS_AddBandFromArray to allow adding a custom noDataValue
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-352'>SEDONA-352</a>] - Refactor MakeEmptyRaster to allow setting custom datatype for the raster
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-360'>SEDONA-360</a>] - Handle nodata values of raster bands in a more concise way
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-365'>SEDONA-365</a>] - Refactor RS_Count to RS_CountValue
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-374'>SEDONA-374</a>] - RS predicates should support (geom, rast) and (rast, rast) as arguments, and use the convex hull of rasters for spatial relationship testing
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-385'>SEDONA-385</a>] - Set the Maven Central to be the first repository to check
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-386'>SEDONA-386</a>] - Speed up GridCoverage2D serialization
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-392'>SEDONA-392</a>] - Add five more pre-commit hooks
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-399'>SEDONA-399</a>] - Support Uber H3 cells
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-400'>SEDONA-400</a>] - pre-commit add hook to ensure that links to vcs websites are permalinks
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-408'>SEDONA-408</a>] - Set a reasonable default size for RasterUDT
</li>
</ul>

### Task

<ul>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-316'>SEDONA-316</a>] - Refactor Sedona Jupyter notebook examples with unified SedonaContext entrypoint
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-317'>SEDONA-317</a>] - Change map visualization in Jupyter notebooks with KeplerGL
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-341'>SEDONA-341</a>] - Move RS_Envelope to GeometryFunctions
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-356'>SEDONA-356</a>] - Change CRS transformation from lat/lon to lon/lat order
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-370'>SEDONA-370</a>] - Completely drop the old GeoTiff reader and writer
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-377'>SEDONA-377</a>] - Change sphere/spheroid functions to work with coordinates in lon/lat order
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-380'>SEDONA-380</a>] - Merge all Sedona Spark module to a single module
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-381'>SEDONA-381</a>] - Merge python-adapter to sql module
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-382'>SEDONA-382</a>] - Merge SQL and Core module to a single Spark module
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-384'>SEDONA-384</a>] - Merge viz module to the spark module
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-397'>SEDONA-397</a>] - Move Map Algebra functions
</li>
</ul>

## Sedona 1.4.1

Sedona 1.4.1 is compiled against Spark 3.3 / Spark 3.4 / Flink 1.12, Java 8.

### Highlights

* [X] **Sedona Spark** More raster functions and bridge RasterUDT and Map Algebra operators. See [Raster based operators](../api/sql/Raster-operators.md#raster-based-operators) and [Raster to Map Algebra operators](../api/sql/Raster-operators.md#raster-to-map-algebra-operators).
* [X] **Sedona Spark & Flink** Added geodesic / geography functions:
    * ST_DistanceSphere
    * ST_DistanceSpheroid
    * ST_AreaSpheroid
    * ST_LengthSpheroid
* [X] **Sedona Spark & Flink** Introduced `SedonaContext` to unify Sedona entry points.
* [X] **Sedona Spark** Support Spark 3.4.
* [X] **Sedona Spark** Added a number of new ST functions.
* [X] **Zeppelin** Zeppelin helium plugin supports plotting geometries like linestring, polygon.

### API change

* **Sedona Spark & Flink** Introduced a new entry point called SedonaContext to unify all Sedona entry points in different compute engines and deprecate old Sedona register entry points. Users no longer have to register Sedona kryo serializer and import many tedious Python classes.
    * **Sedona Spark**:
        * Scala:

        ```scala
        import org.apache.sedona.spark.SedonaContext
        val sedona = SedonaContext.create(SedonaContext.builder().master("local[*]").getOrCreate())
        sedona.sql("SELECT ST_GeomFromWKT(XXX) FROM")
        ```

        * Python:

        ```python
        from sedona.spark import *

        config = SedonaContext.builder().\
           config('spark.jars.packages',
               'org.apache.sedona:sedona-spark-shaded-3.3_2.12:1.4.1,'
               'org.datasyslab:geotools-wrapper:1.4.0-28.2'). \
           getOrCreate()
        sedona = SedonaContext.create(config)
        sedona.sql("SELECT ST_GeomFromWKT(XXX) FROM")
        ```

    * **Sedona Flink**:

    ```java
    import org.apache.sedona.flink.SedonaContext;
    StreamTableEnvironment sedona = SedonaContext.create(env, tableEnv);
    sedona.sqlQuery("SELECT ST_GeomFromWKT(XXX) FROM");
    ```

### Bug

<ul>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-266'>SEDONA-266</a>] - RS_Values throws UnsupportedOperationException for shuffled point arrays
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-267'>SEDONA-267</a>] - Cannot pip install apache-sedona 1.4.0 from source distribution
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-273'>SEDONA-273</a>] - Set a upper bound for Shapely, Pandas and GeoPandas
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-277'>SEDONA-277</a>] - Sedona spark artifacts for scala 2.13 do not have proper POMs
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-283'>SEDONA-283</a>] - Artifacts were deployed twice when running mvn clean deploy
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-284'>SEDONA-284</a>] - Property values in dependency deduced POMs for shaded modules were not substituted
</li>
</ul>

### New Feature

<ul>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-196'>SEDONA-196</a>] - Add ST_Force3D to Sedona
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-239'>SEDONA-239</a>] - Implement ST_NumPoints
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-264'>SEDONA-264</a>] - zeppelin helium plugin supports plotting geometry like linestring, polygon
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-280'>SEDONA-280</a>] - Add ST_GeometricMedian
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-281'>SEDONA-281</a>] - Support geodesic / geography functions
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-286'>SEDONA-286</a>] - Support optimized distance join on ST_DistanceSpheroid and ST_DistanceSphere
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-287'>SEDONA-287</a>] - Use SedonaContext to unify Sedona entry points
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-292'>SEDONA-292</a>] - Bridge Sedona Raster and Map Algebra operators
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-297'>SEDONA-297</a>] - Implement ST_NRings
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-302'>SEDONA-302</a>] - Implement ST_Translate
</li>
</ul>

### Improvement

<ul>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-167'>SEDONA-167</a>] - Add __pycache__ to Python .gitignore
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-265'>SEDONA-265</a>] - Migrate all ST functions to Sedona Inferred Expressions
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-269'>SEDONA-269</a>] - Add data source for writing binary files
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-270'>SEDONA-270</a>] - Remove redundant serialization for rasters
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-271'>SEDONA-271</a>] - Add raster function RS_SRID
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-274'>SEDONA-274</a>] - Move all ST function logics to Sedona common
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-275'>SEDONA-275</a>] - Add raster function RS_SetSRID
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-276'>SEDONA-276</a>] - Add support for Spark 3.4
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-279'>SEDONA-279</a>] - Sedona-Flink should not depend on Sedona-Spark modules
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-282'>SEDONA-282</a>] - R – Add raster write function
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-290'>SEDONA-290</a>] - RDD Spatial Joins should follow the iterator model
</li>
</ul>

## Sedona 1.4.0

Sedona 1.4.0 is compiled against, Spark 3.3 / Flink 1.12, Java 8.

### Highlights

* [X] **Sedona Spark & Flink** Serialize and deserialize geometries 3 - 7X faster
* [X] **Sedona Spark & Flink** Google S2 based spatial join for fast approximate point-in-polygon join. See [Join query in Spark](../api/sql/Optimizer.md#google-s2-based-approximate-equi-join) and [Join query in Flink](../tutorial/flink/sql.md#join-query)
* [X] **Sedona Spark** Pushdown spatial predicate on GeoParquet to reduce memory consumption by 10X: see [explanation](../api/sql/Optimizer.md#Push-spatial-predicates-to-GeoParquet)
* [X] **Sedona Spark** Automatically use broadcast index spatial join for small datasets
* [X] **Sedona Spark** New RasterUDT added to Sedona GeoTiff reader.
* [X] **Sedona Spark** A number of bug fixes and improvement to the Sedona R module.

### API change

* **Sedona Spark & Flink** Packaging strategy changed. See [Maven Coordinate](maven-coordinates.md). Please change your Sedona dependencies if needed. We recommend `sedona-spark-shaded-3.0_2.12-1.4.0` and `sedona-flink-shaded_2.12-1.4.0`
* **Sedona Spark & Flink** GeoTools-wrapper version upgraded. Please use `geotools-wrapper-1.4.0-28.2`.

### Behavior change

* **Sedona Flink** Sedona Flink no longer outputs any LinearRing type geometry. All LinearRing are changed to LineString.
* **Sedona Spark** Join optimization strategy changed. Sedona no longer optimizes spatial join when use a spatial predicate together with an equijoin predicate. By default, it prefers equijoin whenever possible. SedonaConf adds a config option called `sedona.join.optimizationmode`, it can be configured as one of the following values:
	* `all`: optimize all joins having spatial predicate in join conditions. This was the behavior of Apache Sedona prior to 1.4.0.
	* `none`: disable spatial join optimization.
	* `nonequi`: only enable spatial join optimization on non-equi joins. This is the default mode.

When `sedona.join.optimizationmode` is configured as `nonequi`, it won't optimize join queries such as `SELECT * FROM A, B WHERE A.x = B.x AND ST_Contains(A.geom, B.geom)`, since it is an equi-join with equi-condition `A.x = B.x`. Sedona will optimize for `SELECT * FROM A, B WHERE ST_Contains(A.geom, B.geom)`

### Bug

<ul>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-218'>SEDONA-218</a>] - Flaky test caused by improper handling of null struct values in Adapter.toDf
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-221'>SEDONA-221</a>] - Outer join throws NPE for null geometries
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-222'>SEDONA-222</a>] - GeoParquet reader does not work in non-local mode
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-224'>SEDONA-224</a>] - java.lang.NoSuchMethodError when loading GeoParquet files using Spark 3.0.x ~ 3.2.x
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-225'>SEDONA-225</a>] - Cannot count dataframes loaded from GeoParquet files
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-227'>SEDONA-227</a>] - Python SerDe Performance Degradation
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-230'>SEDONA-230</a>] - rdd.saveAsGeoJSON should generate feature properties with field names
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-233'>SEDONA-233</a>] - Incorrect results for several joins in a single stage
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-236'>SEDONA-236</a>] - Flakey python tests in tests.serialization.test_[de]serializers
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-242'>SEDONA-242</a>] - Update jars dependencies in Sedona R to Sedona 1.4.0 version
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-250'>SEDONA-250</a>] - R Deprecate use of Spark 2.4
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-252'>SEDONA-252</a>] - Fix disabled RS_Base64 test
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-255'>SEDONA-255</a>] - R – Translation issue for ST_Point and ST_PolygonFromEnvelope
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-258'>SEDONA-258</a>] - Cannot directly assign raw spatial RDD to CircleRDD using Python binding
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-259'>SEDONA-259</a>] - Adapter.toSpatialRdd in Python binding does not have valid implementation for specifying custom field names for user data
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-261'>SEDONA-261</a>] - Cannot run distance join using broadcast index join when the distance expression references to attributes from the right-side relation
</li>
</ul>

### New Feature

<ul>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-156'>SEDONA-156</a>] - predicate pushdown support for GeoParquet
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-215'>SEDONA-215</a>] - Add ST_ConcaveHull
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-216'>SEDONA-216</a>] - Upgrade jts version to 1.19.0
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-235'>SEDONA-235</a>] - Create ST_S2CellIds in Sedona
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-246'>SEDONA-246</a>] - R GeoTiff read/write
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-254'>SEDONA-254</a>] - R – Add raster type
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-262'>SEDONA-262</a>] - Don&#39;t optimize equi-join by default, add an option to configure when to optimize spatial joins
</li>
</ul>

### Improvement

<ul>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-205'>SEDONA-205</a>] - Use BinaryType in GeometryUDT in Sedona Spark
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-207'>SEDONA-207</a>] - Faster serialization/deserialization of geometry objects
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-212'>SEDONA-212</a>] - Move shading to separate maven modules
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-217'>SEDONA-217</a>] - Automatically broadcast small datasets
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-220'>SEDONA-220</a>] - Upgrade Ubuntu build image from 18.04 to 20.04
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-226'>SEDONA-226</a>] - Support reading and writing GeoParquet file metadata
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-228'>SEDONA-228</a>] - Standardize logging dependencies
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-231'>SEDONA-231</a>] - Redundant Serde Removal
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-234'>SEDONA-234</a>] - ST_Point inconsistencies
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-243'>SEDONA-243</a>] - Improve Sedona R file readers: GeoParquet and Shapefile
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-244'>SEDONA-244</a>] - Align R read/write functions with the Sparklyr framework
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-249'>SEDONA-249</a>] - Add jvm flags for running tests on Java 17
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-251'>SEDONA-251</a>] - Add raster type to Sedona
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-253'>SEDONA-253</a>] - Upgrade geotools to version 28.2
</li>
<li>[<a href='https://issues.apache.org/jira/browse/SEDONA-260'>SEDONA-260</a>] - More intuitive configuration of partition and index-build side of spatial joins in Sedona SQL
</li>
</ul>

## Sedona 1.3.1

This version is a minor release on Sedona 1.3.0 line. It fixes a few critical bugs in 1.3.0. We suggest all 1.3.0 users to migrate to this version.

### Bug fixes

* [SEDONA-204](https://issues.apache.org/jira/browse/SEDONA-204) - Init value in X/Y/Z max should be -Double.MAX
* [SEDONA-206](https://issues.apache.org/jira/browse/SEDONA-206) - Performance regression of ST_Transform in 1.3.0-incubating
* [SEDONA-210](https://issues.apache.org/jira/browse/SEDONA-210) - 1.3.0-incubating doesn't work with Scala 2.12 sbt projects
* [SEDONA-211](https://issues.apache.org/jira/browse/SEDONA-211) - Enforce release managers to use JDK 8
* [SEDONA-201](https://issues.apache.org/jira/browse/SEDONA-201) - Implement ST_MLineFromText and ST_MPolyFromText methods

### New Feature

* [SEDONA-196](https://issues.apache.org/jira/browse/SEDONA-196) - Add ST_Force3D to Sedona
* [SEDONA-197](https://issues.apache.org/jira/browse/SEDONA-197) - Add ST_ZMin, ST_ZMax to Sedona
* [SEDONA-199](https://issues.apache.org/jira/browse/SEDONA-199) - Add ST_NDims to Sedona

### Improvement

* [SEDONA-194](https://issues.apache.org/jira/browse/SEDONA-194) - Merge org.datasyslab.sernetcdf into Sedona
* [SEDONA-208](https://issues.apache.org/jira/browse/SEDONA-208) - Use Spark RuntimeConfig in SedonaConf

!!!note
	Support of Spark 2.X and Scala 2.11 was removed in Sedona 1.3.0+ although some parts of the source code might still be compatible. Sedona 1.3.0+ releases binary for both Scala 2.12 and 2.13.

## Sedona 1.3.0

This version is a major release on Sedona 1.3.0 line and consists of 50 PRs. It includes many new functions, optimization and bug fixes.

### Highlights

* [X] Sedona on Spark in this release is compiled against Spark 3.3.
* [X] Sedona on Flink in this release is compiled against Flink 1.14.
* [X] Scala 2.11 support is removed.
* [X] Spark 2.X support is removed.
* [X] Python 3.10 support is added.
* [X] Aggregators in Flink are added
* [X] Correctness fixes for corner cases in range join and distance join.
* [X] Native GeoParquet read and write (../../tutorial/sql/#load-geoparquet).
    * `df = spark.read.format("geoparquet").option("fieldGeometry", "myGeometryColumn").load("PATH/TO/MYFILE.parquet")`
    * `df.write.format("geoparquet").save("PATH/TO/MYFILE.parquet")`
* [X] DataFrame style API (../../tutorial/sql.md/#dataframe-style-api)
    * `df.select(ST_Point(min_value, max_value).as("point"))`
* [X] Allow WKT format CRS in ST_Transform
    * `ST_Transform(geom, "srcWktString", "tgtWktString")`

```yaml
GEOGCS["WGS 84",
  DATUM["WGS_1984",
  SPHEROID["WGS 84",6378137,298.257223563,
  AUTHORITY["EPSG","7030"]],
  AUTHORITY["EPSG","6326"]],
  PRIMEM["Greenwich",0,
  AUTHORITY["EPSG","8901"]],
  UNIT["degree",0.0174532925199433,
  AUTHORITY["EPSG","9122"]],
  AUTHORITY["EPSG","4326"]]
```

### Bug fixes

  * [SEDONA-119](https://issues.apache.org/jira/browse/SEDONA-119) - ST_Touches join query returns true for polygons whose interiors intersect
  * [SEDONA-136](https://issues.apache.org/jira/browse/SEDONA-136) - Enable testAsEWKT for Flink
  * [SEDONA-137](https://issues.apache.org/jira/browse/SEDONA-137) - Fix ST_Buffer for Flink to work
  * [SEDONA-138](https://issues.apache.org/jira/browse/SEDONA-138) - Fix ST_GeoHash for Flink to work
  * [SEDONA-153](https://issues.apache.org/jira/browse/SEDONA-153) - Python Serialization Fails with Nulls
  * [SEDONA-158](https://issues.apache.org/jira/browse/SEDONA-158) - Fix wrong description about ST_GeometryN in the API docs
  * [SEDONA-169](https://issues.apache.org/jira/browse/SEDONA-169) - Fix ST_RemovePoint in accordance with the API document
  * [SEDONA-178](https://issues.apache.org/jira/browse/SEDONA-178) - Correctness issue in distance join queries
  * [SEDONA-182](https://issues.apache.org/jira/browse/SEDONA-182) - ST_AsText should not return SRID
  * [SEDONA-186](https://issues.apache.org/jira/browse/SEDONA-186) - collecting result rows of a spatial join query with SELECT * fails with serde error
  * [SEDONA-188](https://issues.apache.org/jira/browse/SEDONA-188) - Python warns about missing `jars` even when some are found
  * [SEDONA-193](https://issues.apache.org/jira/browse/SEDONA-193) - ST_AsBinary produces EWKB by mistake

### New Features

  * [SEDONA-94](https://issues.apache.org/jira/browse/SEDONA-94) - GeoParquet  Support For Sedona
  * [SEDONA-125](https://issues.apache.org/jira/browse/SEDONA-125) - Allows customized CRS in ST_Transform
  * [SEDONA-166](https://issues.apache.org/jira/browse/SEDONA-166) - Provide Type-safe DataFrame Style API
  * [SEDONA-168](https://issues.apache.org/jira/browse/SEDONA-168) - Add ST_Normalize to Apache Sedona
  * [SEDONA-171](https://issues.apache.org/jira/browse/SEDONA-171) - Add ST_SetPoint to Apache Sedona

### Improvement

  * [SEDONA-121](https://issues.apache.org/jira/browse/SEDONA-121) - Add equivalent constructors left over from Spark to Flink
  * [SEDONA-132](https://issues.apache.org/jira/browse/SEDONA-132) - Create common module for SQL functions
  * [SEDONA-133](https://issues.apache.org/jira/browse/SEDONA-133) - Allow user-defined schemas in Adapter.toDf()
  * [SEDONA-139](https://issues.apache.org/jira/browse/SEDONA-139) - Fix wrong argument order in Flink unit tests
  * [SEDONA-140](https://issues.apache.org/jira/browse/SEDONA-140) - Update Sedona Dependencies in R Package
  * [SEDONA-143](https://issues.apache.org/jira/browse/SEDONA-143) - Add missing unit tests for the Flink predicates
  * [SEDONA-144](https://issues.apache.org/jira/browse/SEDONA-144) - Add ST_AsGeoJSON to the Flink API
  * [SEDONA-145](https://issues.apache.org/jira/browse/SEDONA-145) - Fix ST_AsEWKT to reserve the Z coordinate
  * [SEDONA-146](https://issues.apache.org/jira/browse/SEDONA-146) - Add missing output functions to the Flink API
  * [SEDONA-147](https://issues.apache.org/jira/browse/SEDONA-147) - Add SRID functions to the Flink API
  * [SEDONA-148](https://issues.apache.org/jira/browse/SEDONA-148) - Add boolean functions to the Flink API
  * [SEDONA-149](https://issues.apache.org/jira/browse/SEDONA-149) - Add Python 3.10 support
  * [SEDONA-151](https://issues.apache.org/jira/browse/SEDONA-151) - Add ST aggregators to Sedona Flink
  * [SEDONA-152](https://issues.apache.org/jira/browse/SEDONA-152) - Add reader/writer functions for GML and KML
  * [SEDONA-154](https://issues.apache.org/jira/browse/SEDONA-154) - Add measurement functions to the Flink API
  * [SEDONA-157](https://issues.apache.org/jira/browse/SEDONA-157) - Add coordinate accessors to the Flink API
  * [SEDONA-159](https://issues.apache.org/jira/browse/SEDONA-159) - Add Nth accessor functions to the Flink API
  * [SEDONA-160](https://issues.apache.org/jira/browse/SEDONA-160) - Fix geoparquetIOTests.scala to cleanup after test
  * [SEDONA-161](https://issues.apache.org/jira/browse/SEDONA-161) - Add ST_Boundary to the Flink API
  * [SEDONA-162](https://issues.apache.org/jira/browse/SEDONA-162) - Add ST_Envelope to the Flink API
  * [SEDONA-163](https://issues.apache.org/jira/browse/SEDONA-163) - Better handle of unsupported types in shapefile reader
  * [SEDONA-164](https://issues.apache.org/jira/browse/SEDONA-164) - Add geometry count functions to the Flink API
  * [SEDONA-165](https://issues.apache.org/jira/browse/SEDONA-165) - Upgrade Apache Rat to 0.14
  * [SEDONA-170](https://issues.apache.org/jira/browse/SEDONA-170) - Add ST_AddPoint and ST_RemovePoint to the Flink API
  * [SEDONA-172](https://issues.apache.org/jira/browse/SEDONA-172) - Add ST_LineFromMultiPoint to Apache Sedona
  * [SEDONA-176](https://issues.apache.org/jira/browse/SEDONA-176) - Make ST_Contains conform with OGC standard, and add ST_Covers and ST_CoveredBy functions.
  * [SEDONA-177](https://issues.apache.org/jira/browse/SEDONA-177) - Support spatial predicates other than INTERSECTS and COVERS/COVERED_BY in RangeQuery.SpatialRangeQuery and JoinQuery.SpatialJoinQuery
  * [SEDONA-181](https://issues.apache.org/jira/browse/SEDONA-181) - Build fails with java.lang.IllegalAccessError: class org.apache.spark.storage.StorageUtils$
  * [SEDONA-189](https://issues.apache.org/jira/browse/SEDONA-189) - Prepare geometries in broadcast join
  * [SEDONA-192](https://issues.apache.org/jira/browse/SEDONA-192) - Null handling in predicates
  * [SEDONA-195](https://issues.apache.org/jira/browse/SEDONA-195) - Add wkt validation and an optional srid to ST_GeomFromWKT/ST_GeomFromText

### Task

* [SEDONA-150](https://issues.apache.org/jira/browse/SEDONA-150) - Drop Spark 2.4 and Scala 2.11 support

## Sedona 1.2.1

This version is a maintenance release on Sedona 1.2.0 line. It includes bug fixes.

Sedona on Spark is now compiled against Spark 3.3, instead of Spark 3.2.

### SQL (for Spark)

Bug fixes:

* [SEDONA-104](https://issues.apache.org/jira/browse/SEDONA-104): Bug in reading band values of GeoTiff images
* [SEDONA-118](https://issues.apache.org/jira/browse/SEDONA-118): Fix the wrong result in ST_Within
* [SEDONA-123](https://issues.apache.org/jira/browse/SEDONA-123): Fix the check for invalid lat/lon in ST_GeoHash

Improvement:

* [SEDONA-96](https://issues.apache.org/jira/browse/SEDONA-96): Refactor ST_MakeValid to use GeometryFixer
* [SEDONA-108](https://issues.apache.org/jira/browse/SEDONA-108): Write support for GeoTiff images
* [SEDONA-122](https://issues.apache.org/jira/browse/SEDONA-122): Overload ST_GeomFromWKB for BYTES column
* [SEDONA-127](https://issues.apache.org/jira/browse/SEDONA-127): Add null safety to ST_GeomFromWKT/WKB/Text
* [SEDONA-129](https://issues.apache.org/jira/browse/SEDONA-129): Support Spark 3.3
* [SEDONA-135](https://issues.apache.org/jira/browse/SEDONA-135): Consolidate and upgrade hadoop dependency

New features:

* [SEDONA-107](https://issues.apache.org/jira/projects/SEDONA/issues/SEDONA-107): Add St_Reverse function
* [SEDONA-105](https://issues.apache.org/jira/browse/SEDONA-105): Add ST_PointOnSurface function
* [SEDONA-95](https://issues.apache.org/jira/browse/SEDONA-95): Add ST_Disjoint predicate
* [SEDONA-112](https://issues.apache.org/jira/browse/SEDONA-112): Add ST_AsEWKT
* [SEDONA-106](https://issues.apache.org/jira/browse/SEDONA-106): Add ST_LineFromText
* [SEDONA-117](https://issues.apache.org/jira/browse/SEDONA-117): Add RS_AppendNormalizedDifference
* [SEDONA-97](https://issues.apache.org/jira/browse/SEDONA-97): Add ST_Force_2D
* [SEDONA-98](https://issues.apache.org/jira/browse/SEDONA-98): Add ST_IsEmpty
* [SEDONA-116](https://issues.apache.org/jira/browse/SEDONA-116): Add ST_YMax and ST_YMin
* [SEDONA-115](https://issues.apache.org/jira/browse/SEDONA-15): Add ST_XMax and ST_Min
* [SEDONA-120](https://issues.apache.org/jira/browse/SEDONA-120): Add ST_BuildArea
* [SEDONA-113](https://issues.apache.org/jira/browse/SEDONA-113): Add ST_PointN
* [SEDONA-124](https://issues.apache.org/jira/browse/SEDONA-124): Add ST_CollectionExtract
* [SEDONA-109](https://issues.apache.org/jira/browse/SEDONA-109): Add ST_OrderingEquals

### Flink

New features:

* [SEDONA-107](https://issues.apache.org/jira/projects/SEDONA/issues/SEDONA-107): Add St_Reverse function
* [SEDONA-105](https://issues.apache.org/jira/browse/SEDONA-105): Add ST_PointOnSurface function
* [SEDONA-95](https://issues.apache.org/jira/browse/SEDONA-95): Add ST_Disjoint predicate
* [SEDONA-112](https://issues.apache.org/jira/browse/SEDONA-112): Add ST_AsEWKT
* [SEDONA-97](https://issues.apache.org/jira/browse/SEDONA-97): Add ST_Force_2D
* [SEDONA-98](https://issues.apache.org/jira/browse/SEDONA-98): Add ST_IsEmpty
* [SEDONA-116](https://issues.apache.org/jira/browse/SEDONA-116): Add ST_YMax and ST_YMin
* [SEDONA-115](https://issues.apache.org/jira/browse/SEDONA-15): Add ST_XMax and ST_Min
* [SEDONA-120](https://issues.apache.org/jira/browse/SEDONA-120): Add ST_BuildArea
* [SEDONA-113](https://issues.apache.org/jira/browse/SEDONA-113): Add ST_PointN
* [SEDONA-110](https://issues.apache.org/jira/browse/SEDONA-110): Add ST_GeomFromGeoHash
* [SEDONA-121](https://issues.apache.org/jira/browse/SEDONA-124): More ST constructors to Flink
* [SEDONA-122](https://issues.apache.org/jira/browse/SEDONA-122): Overload ST_GeomFromWKB for BYTES column

## Sedona 1.2.0

This version is a major release on Sedona 1.2.0 line. It includes bug fixes and new features: Sedona with Apache Flink.

### RDD

Bug fix:

* [SEDONA-18](https://issues.apache.org/jira/browse/SEDONA-18): Fix an error reading Shapefile
* [SEDONA-73](https://issues.apache.org/jira/browse/SEDONA-73): Exclude scala-library from scala-collection-compat

Improvement:

* [SEDONA-77](https://issues.apache.org/jira/browse/SEDONA-77): Refactor Format readers and spatial partitioning functions to be standalone libraries. So they can be used by Flink and others.

### SQL

New features:

* [SEDONA-4](https://issues.apache.org/jira/browse/SEDONA-4): Handle nulls in SQL functions
* [SEDONA-65](https://issues.apache.org/jira/browse/SEDONA-65): Create ST_Difference function
* [SEDONA-68](https://issues.apache.org/jira/browse/SEDONA-68) Add St_Collect function.
* [SEDONA-82](https://issues.apache.org/jira/browse/SEDONA-82): Create ST_SymDifference function
* [SEDONA-75](https://issues.apache.org/jira/browse/SEDONA-75): Add support for "3D" geometries: Preserve Z coordinates on geometries when serializing, ST_AsText, ST_Z, ST_3DDistance
* [SEDONA-86](https://issues.apache.org/jira/browse/SEDONA-86): Support empty geometries in ST_AsBinary and ST_AsEWKB
* [SEDONA-90](https://issues.apache.org/jira/browse/SEDONA-90): Add ST_Union
* [SEDONA-100](https://issues.apache.org/jira/browse/SEDONA-100): Add st_multi function

Bug fix:

* [SEDONA-89](https://issues.apache.org/jira/browse/SEDONA-89): GeometryUDT equals should test equivalence of the other object

### Flink

Major update:

* [SEDONA-80](https://issues.apache.org/jira/browse/SEDONA-80): Geospatial stream processing support in Flink Table API
* [SEDONA-85](https://issues.apache.org/jira/browse/SEDONA-85): ST_Geohash function in Flink
* [SEDONA-87](https://issues.apache.org/jira/browse/SEDONA-87): Support Flink Table and DataStream conversion
* [SEDONA-93](https://issues.apache.org/jira/browse/SEDONA-93): Add ST_GeomFromGeoJSON

## Sedona 1.1.1

This version is a maintenance release on Sedona 1.1.X line. It includes bug fixes and a few new functions.

### Global

New feature:

* [SEDONA-73](https://issues.apache.org/jira/browse/SEDONA-73): Scala source code supports Scala 2.13

### SQL

Bug fix:

* [SEDONA-67](https://issues.apache.org/jira/browse/SEDONA-67): Support Spark 3.2

New features:

* [SEDONA-43](https://issues.apache.org/jira/browse/SEDONA-43): Add ST_GeoHash and ST_GeomFromGeoHash
* [SEDONA-45](https://issues.apache.org/jira/browse/SEDONA-45): Add ST_MakePolygon
* [SEDONA-71](https://issues.apache.org/jira/browse/SEDONA-71): Add ST_AsBinary, ST_AsEWKB, ST_SRID, ST_SetSRID

## Sedona 1.1.0

This version is a major release on Sedona 1.1.0 line. It includes bug fixes and new features: R language API, Raster data and Map algebra support

### Global

Dependency upgrade:

* [SEDONA-30](https://issues.apache.org/jira/browse/SEDONA-30): Use Geotools-wrapper 1.1.0-24.1 to include geotools GeoTiff libraries.

Improvement on join queries in core and SQL:

* [SEDONA-63](https://issues.apache.org/jira/browse/SEDONA-63): Skip empty partitions in NestedLoopJudgement
* [SEDONA-64](https://issues.apache.org/jira/browse/SEDONA-64): Broadcast dedupParams to improve performance

Behavior change:

* [SEDONA-62](https://issues.apache.org/jira/browse/SEDONA-62): Ignore HDF test in order to avoid NASA copyright issue

### Core

Bug fix:

* [SEDONA-41](https://issues.apache.org/jira/browse/SEDONA-41): Fix rangeFilter bug when the leftCoveredByRight para is false
* [SEDONA-53](https://issues.apache.org/jira/browse/SEDONA-53): Fix SpatialKnnQuery NullPointerException

### SQL

Major update:

* [SEDONA-30](https://issues.apache.org/jira/browse/SEDONA-30): Add GeoTiff raster I/O and Map Algebra function

New function:

* [SEDONA-27](https://issues.apache.org/jira/browse/SEDONA-27): Add ST_Subdivide and ST_SubdivideExplode functions

Bug fix:

* [SEDONA-56](https://issues.apache.org/jira/browse/SEDONA-56): Fix broadcast join with Adapter Query Engine enabled
* [SEDONA-22](https://issues.apache.org/jira/browse/SEDONA-22), [SEDONA-60](https://issues.apache.org/jira/browse/SEDONA-60): Fix join queries in SparkSQL when one side has no rows or only one row

### Viz

N/A

### Python

Improvement:

* [SEDONA-59](https://issues.apache.org/jira/browse/SEDONA-59): Make pyspark dependency of Sedona Python optional

Bug fix:

* [SEDONA-50](https://issues.apache.org/jira/browse/SEDONA-50): Remove problematic logging conf that leads to errors on Databricks
* Fix the issue: Spark dependency in setup.py was configured to be < v3.1.0 by mistake.

### R

Major update:

* [SEDONA-31](https://issues.apache.org/jira/browse/SEDONA-31): Add R interface for Sedona

## Sedona 1.0.1

This version is a maintenance release on Sedona 1.0.0 line. It includes bug fixes, some new features, one ==API change==

### Known issue

In Sedona v1.0.1 and earlier versions, the Spark dependency in setup.py was configured to be ==< v3.1.0== [by mistake](https://github.com/apache/sedona/blob/8235924ac80939cbf2ce562b0209b71833ed9429/python/setup.py#L39). When you install Sedona Python (apache-sedona v1.0.1) from PyPI, pip might uninstall PySpark 3.1.1 and install PySpark 3.0.2 on your machine.

Three ways to fix this:

1. After install apache-sedona v1.0.1, uninstall PySpark 3.0.2 and reinstall PySpark 3.1.1

2. Ask pip not to install Sedona dependencies: `pip install --no-deps apache-sedona`

3. Install Sedona from the latest setup.py (on GitHub) manually.

### Global

Dependency upgrade:

* [SEDONA-16](https://issues.apache.org/jira/browse/SEDONA-16): Use a GeoTools Maven Central wrapper to fix failed Jupyter notebook examples
* [SEDONA-29](https://issues.apache.org/jira/browse/SEDONA-29): upgrade to Spark 3.1.1
* [SEDONA-33](https://issues.apache.org/jira/browse/SEDONA-33): jts2geojson version from 0.14.3 to 0.16.1

### Core

Bug fix:

* [SEDONA-35](https://issues.apache.org/jira/browse/SEDONA-35): Address user-data mutability issue with Adapter.toDF()

### SQL

Bug fix:

* [SEDONA-14](https://issues.apache.org/jira/browse/SEDONA-14): Saving dataframe to CSV or Parquet fails due to unknown type
* [SEDONA-15](https://issues.apache.org/jira/browse/SEDONA-15): Add ST_MinimumBoundingRadius and ST_MinimumBoundingCircle functions
* [SEDONA-19](https://issues.apache.org/jira/browse/SEDONA-19): Global indexing does not work with SQL joins
* [SEDONA-20](https://issues.apache.org/jira/browse/SEDONA-20): Case object GeometryUDT and GeometryUDT instance not equal in Spark 3.0.2

New function:

* [SEDONA-21](https://issues.apache.org/jira/browse/SEDONA-21): allows Sedona to be used in pure SQL environment
* [SEDONA-24](https://issues.apache.org/jira/browse/SEDONA-24): Add ST_LineSubString and ST_LineInterpolatePoint
* [SEDONA-26](https://issues.apache.org/jira/browse/SEDONA-26): Add broadcast join support

### Viz

Improvement:

* [SEDONA-32](https://issues.apache.org/jira/browse/SEDONA-32): Speed up ST_Render

API change:

* [SEDONA-29](https://issues.apache.org/jira/browse/SEDONA-29): Upgrade to Spark 3.1.1 and fix ST_Pixelize

### Python

Bug fix:

* [SEDONA-19](https://issues.apache.org/jira/browse/SEDONA-19): Global indexing does not work with SQL joins

## Sedona 1.0.0

This version is the first Sedona release since it joins the Apache Incubator. It includes new functions, bug fixes, and ==API changes==.

### Global

Key dependency upgrade:

* [SEDONA-1](https://issues.apache.org/jira/browse/SEDONA-1): upgrade to JTS 1.18
* upgrade to GeoTools 24.0
* upgrade to jts2geojson 0.14.3

Key dependency packaging strategy change:

* JTS, GeoTools, jts2geojson are no longer packaged in Sedona jars. End users need to add them manually. See [here](maven-coordinates.md).

Key compilation target change:

* [SEDONA-3](https://issues.apache.org/jira/browse/SEDONA-3): Paths and class names have been changed to Apache Sedona
* [SEDONA-7](https://issues.apache.org/jira/browse/SEDONA-7): build the source code for Spark 2.4, 3.0, Scala 2.11, 2.12, Python 3.7, 3.8, 3.9. See [here](compile.md).

### Sedona-core

Bug fix:

* PR [443](https://github.com/apache/sedona/pull/443): read multiple Shape Files by multiPartitions
* PR [451](https://github.com/apache/sedona/pull/451) (==API change==): modify CRSTransform to ignore datum shift

New function:

* [SEDONA-8](https://issues.apache.org/jira/browse/SEDONA-8): spatialRDD.flipCoordinates()

API / behavior change:

* PR [488](https://github.com/apache/sedona/pull/488): JoinQuery.SpatialJoinQuery/DistanceJoinQuery now returns `<Geometry, List>` instead of `<Geometry, HashSet>` because we can no longer use HashSet in Sedona for duplicates removal. All original duplicates in both input RDDs will be preserved in the output.

### Sedona-sql

Bug fix:

* [SEDONA-8](https://issues.apache.org/jira/browse/SEDONA-8) (==API change==): ST_Transform slow due to lock contention.
* PR [427](https://github.com/apache/sedona/pull/427): ST_Point and ST_PolygonFromEnvelope now allows Double type

New function:

* PR [499](https://github.com/apache/sedona/pull/449): ST_Azimuth, ST_X, ST_Y, ST_StartPoint, ST_Boundary, ST_EndPoint, ST_ExteriorRing, ST_GeometryN, ST_InteriorRingN, ST_Dump, ST_DumpPoints, ST_IsClosed, ST_NumInteriorRings, ST_AddPoint, ST_RemovePoint, ST_IsRing
* PR [459](https://github.com/apache/sedona/pull/459): ST_LineMerge
* PR [460](https://github.com/apache/sedona/pull/460): ST_NumGeometries
* PR [469](https://github.com/apache/sedona/pull/469): ST_AsGeoJSON
* [SEDONA-8](https://issues.apache.org/jira/browse/SEDONA-8): ST_FlipCoordinates

Behavior change:

* PR [480](https://github.com/apache/sedona/pull/480): Aggregate Functions rewrite for new Aggregator API. The functions can be used as typed functions in code and enable compilation-time type check.

API change:

* [SEDONA-11](https://issues.apache.org/jira/browse/SEDONA-11): Adapter.toDf() will directly generate a geometry type column. ST_GeomFromWKT is no longer needed.

### Sedona-viz

API change: Drop the function which can generate SVG vector images because the required library has an incompatible license and the SVG image is not good at plotting big data

### Sedona Python

API/Behavior change:

* Python-to-Sedona adapter is moved to a separate module. To use Sedona Python, see [here](install-python.md)

New function:

* PR [448](https://github.com/apache/sedona/pull/448): Add support for partition number in spatialPartitioning function `spatial_rdd.spatialPartitioning(grid_type, NUM_PARTITION)`
