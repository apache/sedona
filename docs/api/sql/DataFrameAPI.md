Sedona SQL functions can be used in a DataFrame style API similar to Spark functions.

The following objects contain the exposed functions: `org.apache.spark.sql.sedona_sql.expressions.st_functions`, `org.apache.spark.sql.sedona_sql.expressions.st_constructors`, `org.apache.spark.sql.sedona_sql.expressions.st_predicates`, and `org.apache.spark.sql.sedona_sql.expressions.st_aggregates`.

Every functions can take all `Column` arguments. Additionally, overloaded forms can commonly take a mix of `String` and other Scala types (such as `Double`) as arguments.

In general the following rules apply (although check the documentation of specific functions for any exceptions):

=== "Scala"
	1. Every function returns a `Column` so that it can be used interchangeably with Spark functions as well as `DataFrame` methods such as `DataFrame.select` or `DataFrame.join`.
	2. Every function has a form that takes all `Column` arguments.
	These are the most versatile of the forms.
	3. Most functions have a form that takes a mix of `String` arguments with other Scala types.

=== "Python"

	1. `Column` type arguments are passed straight through and are always accepted.
	2. `str` type arguments are always assumed to be names of columns and are wrapped in a `Column` to support that.
	If an actual string literal needs to be passed then it will need to be wrapped in a `Column` using `pyspark.sql.functions.lit`.
	3. Any other types of arguments are checked on a per function basis. Generally, arguments that could reasonably support a python native type are accepted and passed through.	4. Shapely `Geometry` objects are not currently accepted in any of the functions.

The exact mixture of argument types allowed is function specific.
However, in these instances, all `String` arguments are assumed to be the names of columns and will be wrapped in a `Column` automatically.
Non-`String` arguments are assumed to be literals that are passed to the sedona function. If you need to pass a `String` literal then you should use the all `Column` form of the sedona function and wrap the `String` literal in a `Column` with the `lit` Spark function.

A short example of using this API (uses the `array_min` and `array_max` Spark functions):

=== "Scala"

	```scala
	val values_df = spark.sql("SELECT array(0.0, 1.0, 2.0) AS values")
	val min_value = array_min("values")
	val max_value = array_max("values")
	val point_df = values_df.select(ST_Point(min_value, max_value).as("point"))
	```

=== "Python"

	```python3
	from pyspark.sql import functions as f
	
	from sedona.sql import st_constructors as stc
	
	df = spark.sql("SELECT array(0.0, 1.0, 2.0) AS values")
	
	min_value = f.array_min("values")
	max_value = f.array_max("values")
	
	df = df.select(stc.ST_Point(min_value, max_value).alias("point"))
	```
	
The above code will generate the following dataframe:
```
+-----------+
|point      |
+-----------+
|POINT (0 2)|
+-----------+
```

Some functions will take native python values and infer them as literals.
For example:

```python3
df = df.select(stc.ST_Point(1.0, 3.0).alias("point"))
```

This will generate a dataframe with a constant point in a column:
```
+-----------+
|point      |
+-----------+
|POINT (1 3)|
+-----------+
```