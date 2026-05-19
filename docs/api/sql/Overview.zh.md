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

# 简介

## 函数列表

SedonaSQL 支持 SQL/MM Part3 空间 SQL 标准，包含以下四种 SQL 算子。所有这些算子都可以通过下面的方式直接调用：

```scala
var myDataFrame = sedona.sql("YOUR_SQL")
```

此外，也可以使用 `expr` 和 `selectExpr` 调用：

```scala
myDataFrame.withColumn("geometry", expr("ST_*")).selectExpr("ST_*")
```

* 构造函数（Constructor）：根据输入的字符串或坐标构造一个 Geometry
	* 示例：ST_GeomFromWKT (string)。根据 WKT 字符串创建一个 Geometry。
	* 文档：[点击查看](Geometry-Functions.md)
* 函数（Function）：在指定的一列或多列上执行某个函数
	* 示例：ST_Distance (A, B)。给定两个 Geometry A 和 B，返回 A 与 B 的欧氏距离。
	* 文档：函数按类别组织。参见 [Geometry Accessors](Geometry-Functions.md)、[Geometry Editors](Geometry-Functions.md)、[Measurement Functions](Geometry-Functions.md#measurement-functions)、[Geometry Processing](Geometry-Functions.md)、[Overlay Functions](Geometry-Functions.md#overlay-functions)，以及侧边栏中的其他分类。
* 聚合函数（Aggregate function）：对给定列返回聚合后的值
	* 示例：ST_Envelope_Aggr (Geometry column)。给定一个 Geometry 列，计算该列所有几何对象的总外接包络。
	* 文档：[点击查看](Geometry-Functions.md#aggregate-functions)
* 谓词（Predicate）：在给定的列上执行逻辑判断，返回 true 或 false
	* 示例：ST_Contains (A, B)。检查 A 是否完全包含 B。如果是则返回 "True"，否则返回 "False"。
	* 文档：[点击查看](Geometry-Functions.md#predicates)

Sedona 还提供了一个 Adapter 用于在 SpatialRDD 与 DataFrame 之间相互转换。详见 [Adapter Scaladoc](../../scaladoc/spark/org/apache/sedona/sql/utils/index.html)

SedonaSQL 支持 SparkSQL 查询优化器，相关文档见[此处](Optimizer.md)

## 栅格函数列表

SedonaSQL 同样支持栅格数据处理。栅格函数以 `RS_` 为前缀。所有栅格算子的调用方式与矢量算子相同：

```scala
var myDataFrame = sedona.sql("YOUR_SQL")
```

* 构造函数（Constructor）：根据输入的文件或参数构造一个 Raster
	* 示例：RS_FromGeoTiff (binary)。从一个 GeoTiff 二进制数据创建一个 Raster。
	* 文档：[点击查看](Raster-Functions.md#raster-constructors)
* 函数（Function）：在指定的一列或多列 Raster 上执行某个函数
	* 示例：RS_Value (raster, point)。给定一个 Raster 和一个 Point 几何对象，返回该位置上的像元值。
	* 文档：函数按类别组织。参见 [Raster Accessors](Raster-Functions.md#raster-accessors)、[Raster Operators](Raster-Functions.md#raster-operators)、[Raster Band Accessors](Raster-Functions.md#raster-band-accessors)、[Raster Output](Raster-Functions.md#raster-output)，以及侧边栏中的其他分类。
* 聚合函数（Aggregate function）：对给定的 Raster 列返回聚合后的值
	* 示例：RS_Union_Aggr (Raster column)。给定一个 Raster 列，将所有 Raster 合并为单个多波段栅格。
	* 文档：[点击查看](Raster-Functions.md#raster-aggregate-functions)
* 谓词（Predicate）：在给定的列上执行逻辑判断，返回 true 或 false
	* 示例：RS_Intersects (raster, geometry)。判断一个栅格是否与一个几何对象相交。如果是则返回 "True"，否则返回 "False"。
	* 文档：[点击查看](Raster-Functions.md#raster-predicates)

## 栅格快速入门

详细说明请参见[编写栅格 DataFrame/SQL 应用](../../tutorial/raster.md)。

## 快速入门

详细说明请参见[编写 SQL/DataFrame 应用](../../tutorial/sql.md)。

1. 在你的项目 pom.xml 或 build.sbt 中加入 Sedona-core 和 Sedona-SQL
2. 如果需要定制 SparkSession，创建你自己的 Sedona 配置。

```scala
import org.apache.sedona.spark.SedonaContext
val config = SedonaContext.builder().
    master("local[*]").appName("SedonaSQL")
    .getOrCreate()
```

3. 在 Sedona context 声明之后加上以下代码：

```scala
import org.apache.sedona.spark.SedonaContext
val sedona = SedonaContext.create(config)
```
