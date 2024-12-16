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

Due to a breaking change in Apache Sedona 1.4.0 to the SQL type of `GeometryUDT`
([SEDONA-205](https://issues.apache.org/jira/browse/SEDONA-205)) as well as the
serialization format of geometry values ([SEDONA-207](https://issues.apache.org/jira/browse/SEDONA-207)), Parquet files
containing geometry columns written by Apache Sedona 1.3.1 or earlier cannot be read by Apache Sedona 1.4.0 or later.

For parquet files written by `"parquet"` format when using Apache Sedona 1.3.1-incubating or earlier:

```python
df.write.format("parquet").save("path/to/parquet/files")
```

Reading such files with Apache Sedona 1.4.0 or later using `spark.read.format("parquet").load("path/to/parquet/files")` will result in an exception:

```
24/01/08 12:52:56 ERROR Executor: Exception in task 0.0 in stage 12.0 (TID 11)
org.apache.spark.sql.AnalysisException: Invalid Spark read type: expected required group geom (LIST) {
  repeated group list {
    required int32 element (INTEGER(8,true));
  }
} to be list type but found Some(BinaryType)
	at org.apache.spark.sql.execution.datasources.parquet.ParquetSchemaConverter$.checkConversionRequirement(ParquetSchemaConverter.scala:745)
	at org.apache.spark.sql.execution.datasources.parquet.ParquetToSparkSchemaConverter.$anonfun$convertGroupField$3(ParquetSchemaConverter.scala:343)
	at scala.Option.fold(Option.scala:251)
	at org.apache.spark.sql.execution.datasources.parquet.ParquetToSparkSchemaConverter.convertGroupField(ParquetSchemaConverter.scala:324)
	at org.apache.spark.sql.execution.datasources.parquet.ParquetToSparkSchemaConverter.convertField(ParquetSchemaConverter.scala:188)
	at org.apache.spark.sql.execution.datasources.parquet.ParquetToSparkSchemaConverter.$anonfun$convertInternal$3(ParquetSchemaConverter.scala:147)
	at org.apache.spark.sql.execution.datasources.parquet.ParquetToSparkSchemaConverter.$anonfun$convertInternal$3$adapted(ParquetSchemaConverter.scala:117)
	at scala.collection.TraversableLike.$anonfun$map$1(TraversableLike.scala:286)
	at scala.collection.immutable.Range.foreach(Range.scala:158)
	at scala.collection.TraversableLike.map(TraversableLike.scala:286)
	at scala.collection.TraversableLike.map$(TraversableLike.scala:279)
	at scala.collection.AbstractTraversable.map(Traversable.scala:108)
	...
```

Since v1.5.1, GeoParquet supports reading legacy Parquet files. you can use `"geoparquet"` format with the `.option("legacyMode", "true")` option. Here is an example:

=== "Scala/Java"

    ```scala
    val df = sedona.read.format("geoparquet").option("legacyMode", "true").load("path/to/legacy-parquet-files")
    ```

=== "Java"

    ```java
    Dataset<Row> df = sedona.read.format("geoparquet").option("legacyMode", "true").load("path/to/legacy-parquet-files")
    ```

=== "Python"

    ```python
    df = sedona.read.format("geoparquet").option("legacyMode", "true").load("path/to/legacy-parquet-files")
    ```
