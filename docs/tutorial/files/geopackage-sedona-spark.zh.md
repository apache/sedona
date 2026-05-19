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

# 在 Spark 上使用 Apache Sedona 处理 GeoPackage

本文介绍如何使用 Apache Sedona 与 Spark 读取 GeoPackage 文件。

您将了解 GeoPackage 文件格式的优缺点，以及如何在生产环境中使用它。

下面先创建一个 GeoPackage 文件，再演示读取。

## 使用 Sedona 与 Spark 读取 GeoPackage 文件

先创建一个包含若干行数据的 GeoPackage 文件。

首先构造一个 GeoPandas DataFrame：

```python
point1 = Point(0, 0)
point2 = Point(1, 1)
polygon1 = Polygon([(5, 5), (6, 6), (7, 5), (6, 4)])

data = {
    "name": ["Point A", "Point B", "Polygon A"],
    "value": [10, 20, 30],
    "geometry": [point1, point2, polygon1],
}
gdf = gpd.GeoDataFrame(data, geometry="geometry")
```

将 GeoPandas DataFrame 写入 GeoPackage 文件：

```python
gdf.to_file("/tmp/my_file.gpkg", layer="my_layer", driver="GPKG")
```

代码中将 driver 设为 `GPKG`，因此 GeoPandas 会以 GeoPackage 格式写出。

可以把 layer 视作表名。

接下来用 Apache Sedona 与 Spark 读取这个 GeoPackage 文件：

```python
df = (
    sedona.read.format("geopackage")
    .option("tableName", "my_layer")
    .load("/tmp/my_file.gpkg")
)
df.show()
```

DataFrame 内容如下：

```
+---+--------------------+---------+-----+
|fid|                geom|     name|value|
+---+--------------------+---------+-----+
|  1|         POINT (0 0)|  Point A|   10|
|  2|         POINT (1 1)|  Point B|   20|
|  3|POLYGON ((5 5, 6 ...|Polygon A|   30|
+---+--------------------+---------+-----+
```

几何列可以包含点、多边形等多种几何对象。

也可以查看 GeoPackage 文件的元数据：

```python
df = (
    sedona.read.format("geopackage")
    .option("showMetadata", "true")
    .load("/tmp/my_file.gpkg")
)
df.show()
```

输出如下：

```
+----------+---------+----------+-----------+--------------------+-----+-----+-----+-----+------+
|table_name|data_type|identifier|description|         last_change|min_x|min_y|max_x|max_y|srs_id|
+----------+---------+----------+-----------+--------------------+-----+-----+-----+-----+------+
|  my_layer| features|  my_layer|           |2025-02-25 06:28:...|  0.0|  0.0|  7.0|  6.0| 99999|
+----------+---------+----------+-----------+--------------------+-----+-----+-----+-----+------+
```

## 使用 Sedona 与 Spark 读取多个 GeoPackage 文件

Sedona 也支持读取多个 GeoPackage 文件。假设有以下文件结构：

```
gpkgs/
  my_file1.gpkg
  my_file2.gpkg
```

可以这样读取所有文件：

```python
df = sedona.read.format("geopackage").option("tableName", "my_layer").load("/tmp/gpkgs")
df.show()
```

结果如下：

```
+---+--------------------+---------+-----+
|fid|                geom|     name|value|
+---+--------------------+---------+-----+
|  1|         POINT (5 5)|  Point C|   30|
|  2|POLYGON ((5 5, 6 ...|Polygon A|   40|
|  1|         POINT (0 0)|  Point A|   10|
|  2|         POINT (1 1)|  Point B|   20|
+---+--------------------+---------+-----+
```

只需指定包含 GeoPackage 文件的目录，Sedona 即可将它们全部加载到一个 DataFrame 中。

由于 Sedona 可以并行读取与处理这些文件，因此非常适合分析大量 GeoPackage 文件。

## 加载 GeoPackage 中的栅格数据

也可以从 GeoPackage 中的栅格表加载数据。代码如下：

```python
df = (
    sedona.read.format("geopackage")
    .option("tableName", "raster_table")
    .load("/path/to/geopackage")
)
```

DataFrame 内容如下：

```
+---+----------+-----------+--------+--------------------+
| id|zoom_level|tile_column|tile_row|           tile_data|
+---+----------+-----------+--------+--------------------+
|  1|        11|        428|     778|GridCoverage2D["c...|
|  2|        11|        429|     778|GridCoverage2D["c...|
|  3|        11|        428|     779|GridCoverage2D["c...|
|  4|        11|        429|     779|GridCoverage2D["c...|
|  5|        11|        427|     777|GridCoverage2D["c...|
+---+----------+-----------+--------+--------------------+
```

已知限制（v1.7.0）：

* 不支持 webp 栅格
* 不支持 ewkb 几何
* 不支持基于几何包络的过滤

以上限制都将在后续版本中陆续解决，敬请关注！

## GeoPackage 文件格式的优势

GeoPackage 格式有许多优点：

* 因为是开放格式，任何引擎都可以支持。
* 与许多其他格式不同，它是可变的。
* 不像某些格式，它会保存 CRS 信息。
* 既可以存储矢量数据，也可以存储栅格数据。
* GeoPandas、Sedona、SQLite 等众多引擎都能读取。

但 GeoPackage 也存在不少不足。

## GeoPackage 的劣势

GeoPackage 文件格式有以下劣势：

* 行式存储，无法享受列式格式的列裁剪优势。
* 不支持多引擎并发事务。
* 虽然支持 SQLite 事务，但跨引擎可靠事务很难实现。
* 并非所有引擎都完整支持。

## 结论

如果您本身在使用 SQLite，GeoPackage 是非常稳健的格式选择。

Sedona 能读取由 SQLite 分析生成的 GeoPackage 文件这一点非常有价值——可以并行读取这些文件，并对海量数据进行分析；同时也可以在集群上运行 Sedona。

如果您还没有使用过 GeoPackage，那么使用 GeoParquet、Iceberg 这类格式通常会是更好的选择。
