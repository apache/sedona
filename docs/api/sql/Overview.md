# Introduction

## Function list

SedonaSQL supports SQL/MM Part3 Spatial SQL Standard. It includes four kinds of SQL operators as follows. All these operators can be directly called through:

```scala
var myDataFrame = sedona.sql("YOUR_SQL")
```

Alternatively, `expr` and `selectExpr` can be used:

```scala
myDataFrame.withColumn("geometry", expr("ST_*")).selectExpr("ST_*")
```

* Constructor: Construct a Geometry given an input string or coordinates
	* Example: ST_GeomFromWKT (string). Create a Geometry from a WKT String.
	* Documentation: [Here](Constructor.md)
* Function: Execute a function on the given column or columns
	* Example: ST_Distance (A, B). Given two Geometry A and B, return the Euclidean distance of A and B.
	* Documentation: [Here](Function.md)
* Aggregate function: Return the aggregated value on the given column
	* Example: ST_Envelope_Aggr (Geometry column). Given a Geometry column, calculate the entire envelope boundary of this column.
	* Documentation: [Here](AggregateFunction.md)
* Predicate: Execute a logic judgement on the given columns and return true or false
	* Example: ST_Contains (A, B). Check if A fully contains B. Return "True" if yes, else return "False".
	* Documentation: [Here](Predicate.md)

Sedona also provides an Adapter to convert SpatialRDD <-> DataFrame. Please read [Adapter Scaladoc](../../scaladoc/spark/org/apache/sedona/sql/utils/index.html)

SedonaSQL supports SparkSQL query optimizer, documentation is [Here](Optimizer.md)

## Quick start

The detailed explanation is here [Write a SQL/DataFrame application](../../tutorial/sql.md).

1. Add Sedona-core and Sedona-SQL into your project pom.xml or build.sbt
2. Create your Sedona config if you want to customize your SparkSession.

```scala
import org.apache.sedona.spark.SedonaContext
val config = SedonaContext.builder().
    master("local[*]").appName("SedonaSQL")
    .getOrCreate()
```

3. Add the following line after your Sedona context declaration:

```scala
import org.apache.sedona.spark.SedonaContext
val sedona = SedonaContext.create(config)
```
