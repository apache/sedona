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

# 使用 Spark 读写带几何对象的 Apache Sedona CSV

本文展示如何使用 Sedona 与 Spark 读写带几何列的 CSV 文件。

您将了解 CSV 在存储几何数据时的优缺点。

先看如何写出带几何数据的 CSV。

## 使用 Sedona 与 Spark 写出带几何对象的 CSV

先用 Sedona 与 Spark 创建一个 DataFrame：

```python
df = sedona.createDataFrame(
    [
        ("a", "LINESTRING(2.0 5.0,6.0 1.0)"),
        ("b", "POINT(1.0 2.0)"),
        ("c", "POLYGON((7.0 1.0,7.0 3.0,9.0 3.0,7.0 1.0))"),
    ],
    ["id", "geometry"],
)
df = df.withColumn("geometry", ST_GeomFromText(col("geometry")))
```

DataFrame 的内容如下：

```
+---+------------------------------+
|id |geometry                      |
+---+------------------------------+
|a  |LINESTRING (2 5, 6 1)         |
|b  |POINT (1 2)                   |
|c  |POLYGON ((7 1, 7 3, 9 3, 7 1))|
+---+------------------------------+
```

将 DataFrame 写入 CSV 文件：

```python
df = df.withColumn("geom_wkt", ST_AsText(col("geometry"))).drop("geometry")
df.repartition(1).write.option("header", True).format("csv").mode("overwrite").save(
    "/tmp/my_csvs"
)
```

注意这里使用 `repartition(1)` 把 DataFrame 输出为单个文件。生产环境通常更建议并行输出多个文件以提升写入速度，这里仅为示例方便。

CSV 文件内容如下：

```
id,geom_wkt
a,"LINESTRING (2 5, 6 1)"
b,POINT (1 2)
c,"POLYGON ((7 1, 7 3, 9 3, 7 1))"
```

`geom_wkt` 列以纯文本保存，便于人工查看；它使用标准 WKT 格式，任何能解析 WKT 的引擎都能读取该列。

## 使用 Sedona 与 Spark 读取带几何对象的 CSV

将 CSV 文件读回 DataFrame：

```python
df = (
    sedona.read.option("header", True)
    .format("CSV")
    .load("/tmp/my_csvs")
    .withColumn("geometry", ST_GeomFromText(col("geom_wkt")))
    .drop("geom_wkt")
)
```

文件中的 `geom_wkt` 列保存为文本，因此读取时需用 `ST_GeomFromText` 转换为几何列。DataFrame 的内容如下：

```
+---+------------------------------+
|id |geometry                      |
+---+------------------------------+
|a  |LINESTRING (2 5, 6 1)         |
|b  |POINT (1 2)                   |
|c  |POLYGON ((7 1, 7 3, 9 3, 7 1))|
+---+------------------------------+
```

确认 schema 正确：

```
root
 |-- id: string (nullable = true)
 |-- geometry: geometry (nullable = true)
```

## 使用 Extended Well-Known Text（EWKT）读写 CSV

下面看如何把 DataFrame 以 EWKT 写入 CSV。先给几何列设置 SRID：

```python
df = df.withColumn("geometry", ST_SetSRID(col("geometry"), 4326))
```

然后将 DataFrame 以 EWKT 列写出：

```python
df = df.withColumn("geom_ewkt", ST_AsEWKT(col("geometry"))).drop("geometry")
df.repartition(1).write.option("header", True).format("csv").mode("overwrite").save(
    "/tmp/my_ewkt_csvs"
)
```

CSV 文件内容如下：

```
id,geom_ewkt
a,"SRID=4326;LINESTRING (2 5, 6 1)"
b,SRID=4326;POINT (1 2)
c,"SRID=4326;POLYGON ((7 1, 7 3, 9 3, 7 1))"
```

读回带 EWKT 列的 CSV：

```python
df = (
    sedona.read.option("header", True)
    .format("csv")
    .load("/tmp/my_ewkt_csvs")
    .withColumn("geometry", ST_GeomFromEWKT(col("geom_ewkt")))
    .drop("geom_ewkt")
)
```

DataFrame 的内容如下：

```
+---+------------------------------+
|id |geometry                      |
+---+------------------------------+
|a  |LINESTRING (2 5, 6 1)         |
|b  |POINT (1 2)                   |
|c  |POLYGON ((7 1, 7 3, 9 3, 7 1))|
+---+------------------------------+
```

打印 Sedona DataFrame 时不会显示 SRID，但该元数据已在内部保留。

## CSV 用于几何数据的优势

使用 CSV 存储几何数据有以下优势：

* 大多数引擎都支持 CSV
* 人工可读
* 借助 “扩展” 格式（EWKT）可保存 CRS 信息
* 标准经历了长期考验

但 CSV 也有不少劣势。

## CSV 用于几何数据的劣势

将几何数据存储为 CSV 文件有以下劣势：

* CSV 是行式存储，引擎在读取时无法只挑选个别列。列式格式支持的列裁剪是重要的性能特性。
* 行式特性也使得 CSV 难以高效压缩。
* CSV 不包含 schema，引擎要么进行 schema 推断，要么用户在读取时手动指定。schema 推断容易出错，手动指定又繁琐。
* CSV 不存储 row group 元数据，无法跳过 row group。
* CSV 不存储文件级元数据，无法跳过整个文件。
* 即使保留了 SRID 元数据，也只能写在 CSV 的每一行上，由于 CSV 不支持文件级元数据，这会造成不必要的空间浪费。

## 结论

Spark 与 Sedona 支持以 CSV 存储几何数据，但通常较慢，建议仅在必要时使用。

如果您要构建地理空间数据湖，GeoParquet 几乎总是更好的选择。

如果您要构建地理空间数据湖仓（lakehouse），Iceberg 也是不错的选项。
