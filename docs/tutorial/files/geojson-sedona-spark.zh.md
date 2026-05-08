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

# 在 Spark 上使用 Apache Sedona 处理 GeoJSON

本文介绍如何使用 Apache Sedona 与 Spark 读写单行 GeoJSON 与多行 GeoJSON 文件。

末尾给出 GeoJSON 在空间分析中的优缺点小结。

GeoJSON 基于 JSON，支持以下类型：

* Point
* LineString
* Polygon
* MultiPoint
* MultiLineString
* MultiPolygon

更多关于 [GeoJSON 格式的规范](https://datatracker.ietf.org/doc/html/rfc7946) 见此处。

## 使用 Sedona 与 Spark 读取多行 GeoJSON

读取多行 GeoJSON 的方式如下：

```python
df = (
    sedona.read.format("geojson")
    .option("multiLine", "true")
    .load("data/multiline_geojson.json")
    .selectExpr("explode(features) as features")
    .select("features.*")
    .withColumn("prop0", expr("properties['prop0']"))
    .drop("properties")
    .drop("type")
)
df.show(truncate=False)
```

输出如下：

```
+---------------------------------------------+------+
|geometry                                     |prop0 |
+---------------------------------------------+------+
|POINT (102 0.5)                              |value0|
|LINESTRING (102 0, 103 1, 104 0, 105 1)      |value1|
|POLYGON ((100 0, 101 0, 101 1, 100 1, 100 0))|value2|
+---------------------------------------------+------+
```

该多行 GeoJSON 文件包含一个点、一个折线和一个多边形。原始文件内容如下：

```json
{ "type": "FeatureCollection",
    "features": [
      { "type": "Feature",
        "geometry": {"type": "Point", "coordinates": [102.0, 0.5]},
        "properties": {"prop0": "value0"}
        },
      { "type": "Feature",
        "geometry": {
          "type": "LineString",
          "coordinates": [
            [102.0, 0.0], [103.0, 1.0], [104.0, 0.0], [105.0, 1.0]
            ]
          },
        "properties": {
          "prop0": "value1",
          "prop1": 0.0
          }
        },
      { "type": "Feature",
         "geometry": {
           "type": "Polygon",
           "coordinates": [
             [ [100.0, 0.0], [101.0, 0.0], [101.0, 1.0],
               [100.0, 1.0], [100.0, 0.0] ]
             ]
         },
         "properties": {
           "prop0": "value2",
           "prop1": {"this": "that"}
           }
         }
       ]
}
```

注意整体结构是一个 `FeatureCollection`，每个 feature 都有几何类型、几何坐标与属性字段。

也可以一次读取多个多行 GeoJSON 文件。假设有如下目录：

```
many_geojsons/
  file1.json
  file2.json
```

读取方法如下：

```python
df = (
    sedona.read.format("geojson").option("multiLine", "true").load("data/many_geojsons")
)
```

只需把包含 JSON 文件的目录传入即可。

多行 GeoJSON 对人类阅读友好，但对机器低效。建议把 JSON 数据写在单行上。

## 使用 Sedona 与 Spark 读取单行 GeoJSON

读取单行 GeoJSON 的方式如下：

```python
df = (
    sedona.read.format("geojson")
    .load("data/singleline_geojson.json")
    .withColumn("prop0", expr("properties['prop0']"))
    .drop("properties")
    .drop("type")
)
df.show(truncate=False)
```

结果如下：

```
+---------------------------------------------+------+
|geometry                                     |prop0 |
+---------------------------------------------+------+
|POINT (102 0.5)                              |value0|
|LINESTRING (102 0, 103 1, 104 0, 105 1)      |value1|
|POLYGON ((100 0, 101 0, 101 1, 100 1, 100 0))|value2|
+---------------------------------------------+------+
```

数据如下：

```
{"type":"Feature","geometry":{"type":"Point","coordinates":[102.0,0.5]},"properties":{"prop0":"value0"}}
{"type":"Feature","geometry":{"type":"LineString","coordinates":[[102.0,0.0],[103.0,1.0],[104.0,0.0],[105.0,1.0]]},"properties":{"prop0":"value1"}}
{"type":"Feature","geometry":{"type":"Polygon","coordinates":[[[100.0,0.0],[101.0,0.0],[101.0,1.0],[100.0,1.0],[100.0,0.0]]]},"properties":{"prop0":"value2"}}
```

可以看出：多行 GeoJSON 使用一个 `FeatureCollection`，而单行 GeoJSON 中每行是独立的 `Feature`。

单行 GeoJSON 文件更优——查询引擎可以对它进行 split。

下面看如何用 Sedona 通过 DataFrame 写出 GeoJSON。

## 使用 Sedona 与 Spark 写出 GeoJSON

创建一个 Sedona DataFrame，再写出为 GeoJSON：

```
df = sedona.createDataFrame([
    ("a", 'LINESTRING(2.0 5.0,6.0 1.0)'),
    ("b", 'LINESTRING(7.0 4.0,9.0 2.0)'),
    ("c", 'LINESTRING(1.0 3.0,3.0 1.0)'),
], ["id", "geometry"])
actual = df.withColumn("geometry", ST_GeomFromText(col("geometry")))
actual.write.format("geojson").mode("overwrite").save("/tmp/a_thing")
```

写出的文件如下：

```
a_thing/
  _SUCCESS
  part-00000-856044c5-ae35-4306-bf7a-ae9c3cb25434-c000.json
  part-00003-856044c5-ae35-4306-bf7a-ae9c3cb25434-c000.json
  part-00007-856044c5-ae35-4306-bf7a-ae9c3cb25434-c000.json
  part-00011-856044c5-ae35-4306-bf7a-ae9c3cb25434-c000.json
```

Sedona 会并行写出多个 GeoJSON 文件，比写单一文件更快。

注意：写出操作要求 DataFrame 至少包含一个几何类型的列。Sedona 会按以下规则确定使用哪一列作为几何列：

1. 如果存在名为 “geometry” 且类型为 geometry 的列，则使用该列；
2. 否则使用根 schema 中找到的第一个几何列。

也可以通过 `geometry.column` 选项手动指定使用哪一列：

```python
df.write.format("geojson").option("geometry.column", "geometry").save("/tmp/a_thing")
```

将这些 GeoJSON 文件再读回 DataFrame：

```python
df = sedona.read.format("geojson").load("/tmp/a_thing")
df.show(truncate=False)
```

```
+---------------------+----------+-------+
|geometry             |properties|type   |
+---------------------+----------+-------+
|LINESTRING (1 3, 3 1)|{c}       |Feature|
|LINESTRING (2 5, 6 1)|{a}       |Feature|
|LINESTRING (7 4, 9 2)|{b}       |Feature|
+---------------------+----------+-------+
```

## GeoJSON 的优势

GeoJSON 格式有以下优点：

* 人工可读
* 可以并行写出多个文件，便于并行处理引擎获得更快的 I/O。
* 大量引擎都支持 GeoJSON / JSON 文件。

不过 GeoJSON 也有不少缺点，使其在存储地理空间数据时并非最佳选择。

## GeoJSON 的局限

GeoJSON 在空间数据湖场景下可能存在以下性能问题：

* GeoJSON 对象虽然可以包含 CRS，但 CRS 是可选的，这一关键信息可能丢失。
* 行式存储，无法享受 GeoParquet 等列式格式的列裁剪等性能优化。
* 不存储 row group 元数据，无法进行 row-group 过滤（这是 Parquet 的一项性能优化）。
* 文件尾部不携带 schema，需手动提供或自动推断。
* GeoJSON 规范要求固定的结构，对某些数据集而言比较僵硬。
* 只能用于构建数据湖，无法用于构建数据湖仓（lakehouse）。

## 结论

GeoJSON 在空间数据分析中很常见，Apache Sedona 提供完整的读写支持是非常方便的。

GeoJSON 受到广泛支持且可读性好，但相比 GeoParquet 等格式速度较慢。一般而言，进行空间数据分析时建议优先使用 GeoParquet 或 Iceberg 以获得更好的性能。
