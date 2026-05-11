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

由于 Apache Sedona 1.4.0 对 `GeometryUDT` 的 SQL 类型
（[SEDONA-205](https://issues.apache.org/jira/browse/SEDONA-205)）以及几何值的序列化格式
（[SEDONA-207](https://issues.apache.org/jira/browse/SEDONA-207)）引入了破坏性变更，因此由 Apache Sedona 1.3.1 或更早版本写出的、包含几何列的 Parquet 文件无法被 Apache Sedona 1.4.0 或更高版本直接读取。

对于在 Apache Sedona 1.3.1-incubating 或更早版本下、使用 `"parquet"` 格式写出的 parquet 文件：

```python
df.write.format("parquet").save("path/to/parquet/files")
```

如果用 Apache Sedona 1.4.0 或更高版本通过 `spark.read.format("parquet").load("path/to/parquet/files")` 来读取这些文件，将会抛出异常：

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

自 v1.5.1 起，GeoParquet 支持读取旧版 Parquet 文件。你可以使用 `"geoparquet"` 格式，并加上 `.option("legacyMode", "true")` 选项。示例如下：

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
