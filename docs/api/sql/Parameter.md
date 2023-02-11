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
## Explanation

* sedona.global.index
	* Use spatial index (currently, only supports in SQL range join and SQL distance join)
	* Default: true
	* Possible values: true, false
* sedona.global.indextype
	* Spatial index type, only valid when "sedona.global.index" is true
	* Default: quadtree
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
