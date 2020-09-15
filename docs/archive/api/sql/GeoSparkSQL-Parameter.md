## Usage
GeoSparkSQL supports many parameters. To change their values,

1. Set it through SparkConf:
```Scala
sparkSession = SparkSession.builder().
      config("spark.serializer",classOf[KryoSerializer].getName).
      config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName).
      config("geospark.global.index","true")
      master("local[*]").appName("myGeoSparkSQLdemo").getOrCreate()
```
2. Check your current GeoSparkSQL configuration:
```Scala
val geosparkConf = new GeoSparkConf(sparkSession.sparkContext.getConf)
println(geosparkConf)
```
## Explanation

* geospark.global.index
	* Use spatial index (currently, only supports in SQL range join and SQL distance join)
	* Default: true
	* Possible values: true, false
* geospark.global.indextype
	* Spatial index type, only valid when "geospark.global.index" is true
	* Default: rtree
	* Possible values: rtree, quadtree
* geospark.join.gridtype
	* Spatial partitioning grid type for join query
	* Default: quadtree
	* Possible values: quadtree, kdbtree, rtree, voronoi
* geospark.join.numpartition **(Advanced users only!)**
	* Number of partitions for both sides in a join query
	* Default: -1, which means use the existing partitions
	* Possible values: any integers
* geospark.join.indexbuildside **(Advanced users only!)**
	* The side which GeoSpark builds spatial indices on
	* Default: left
	* Possible values: left, right
* geospark.join.spatitionside **(Advanced users only!)**
	* The dominant side in spatial partitioning stage
	* Default: left
	* Possible values: left, right