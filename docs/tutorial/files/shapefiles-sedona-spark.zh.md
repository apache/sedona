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

# 在 Spark 上使用 Apache Sedona 处理 Shapefile

本文介绍如何使用 Apache Sedona 与 Spark 读取 Shapefile。

Shapefile 是 “Esri 矢量数据存储格式，用于存储地理要素的位置、形状与属性”。Shapefile 格式由 Esri 私有，但 [规范是公开的](https://www.esri.com/content/dam/esrisites/sitecore-archive/Files/Pdfs/library/whitepapers/pdfs/shapefile.pdf)。

Shapefile 有不少限制，但仍被广泛使用，因此 Sedona 能读取它非常有价值。

下面看如何用 Sedona 与 Spark 读取 Shapefile。

## 使用 Sedona 与 Spark 读取 Shapefile

先用 GeoPandas 与 Shapely 创建一个 Shapefile：

```python
import geopandas as gpd
from shapely.geometry import Point

point1 = Point(0, 0)
point2 = Point(1, 1)

data = {"name": ["Point A", "Point B"], "value": [10, 20], "geometry": [point1, point2]}

gdf = gpd.GeoDataFrame(data, geometry="geometry")
gdf.to_file("/tmp/my_geodata.shp")
```

输出的文件如下：

```
/tmp/
  my_geodata.cpg
  my_geodata.dbf
  my_geodata.shp
  my_geodata.shx
```

Shapefile 并不是单一文件，其数据分散在多个文件中。

将 Shapefile 读入由 Spark 驱动的 Sedona DataFrame：

```python
df = sedona.read.format("shapefile").load("/tmp/my_geodata.shp")
df.show()
```

```
+-----------+-------+-----+
|   geometry|   name|value|
+-----------+-------+-----+
|POINT (0 0)|Point A|   10|
|POINT (1 1)|Point B|   20|
+-----------+-------+-----+
```

也可以一并读取每行的唯一 record number：

```python
df = (
    sedona.read.format("shapefile")
    .option("key.name", "FID")
    .load("/tmp/my_geodata.shp")
)
```

```
+-----------+---+-------+-----+
|   geometry|FID|   name|value|
+-----------+---+-------+-----+
|POINT (0 0)|  1|Point A|   10|
|POINT (1 1)|  2|Point B|   20|
+-----------+---+-------+-----+
```

几何列默认名为 `geometry`。可通过 `geometry.name` 选项修改。如果某个非空间属性恰好叫 `geometry`，则必须配置 `geometry.name` 以避免冲突：

```python
df = (
    sedona.read.format("shapefile")
    .option("geometry.name", "geom")
    .load("/path/to/shapefile")
)
```

字符串属性的字符编码会从 `.cpg` 文件推断。如果字符串字段出现乱码，可以通过 `charset` 选项手动指定正确的字符集，例如：

=== "Scala/Java"

    ```scala
    val df = sedona.read.format("shapefile").option("charset", "UTF-8").load("/path/to/shapefile")
    ```

=== "Java"

    ```java
    Dataset<Row> df = sedona.read().format("shapefile").option("charset", "UTF-8").load("/path/to/shapefile")
    ```

=== "Python"

    ```python
    df = (
        sedona.read.format("shapefile")
        .option("charset", "UTF-8")
        .load("/path/to/shapefile")
    )
    ```

下面看如何在 Sedona DataFrame 中加载多个 Shapefile。

## 使用 Sedona 加载多个 Shapefile

假设有如下目录结构：

```
/tmp/shapefiles/
  file1.cpg
  file1.dbf
  file1.shp
  file1.shx
  file2.cpg
  file2.dbf
  file2.shp
  file2.shx
```

目录中包含 2 个 `.shp` 以及对应的辅助文件。

可以这样把多个 Shapefile 加载到 Sedona DataFrame 中：

```python
df = sedona.read.format("shapefile").load("/tmp/shapefiles")
df.show()
```

```
+-----------+-------+-----+
|   geometry|   name|value|
+-----------+-------+-----+
|POINT (0 0)|Point A|   10|
|POINT (1 1)|Point B|   20|
|POINT (2 2)|Point C|   10|
|POINT (3 3)|Point D|   20|
+-----------+-------+-----+
```

只需将 Shapefile 所在目录传入，Sedona 即可识别加载。

输入路径既可以是包含一个或多个 Shapefile 的目录，也可以是 `.shp` 文件本身：

* 输入是目录时，会加载该目录下直接存在的所有 Shapefile。如果还需要加载子目录中的 Shapefile，请加上 `.option("recursiveFileLookup", "true")`。
* 输入是 `.shp` 文件时，Sedona 会自动查找同名的 `.dbf`、`.shx` 等同伴文件并一并加载。

## Shapefile 的优势

Shapefile 与 Esri 生态深度集成，并被大量服务广泛使用。

可以从 Esri 输出 Shapefile，再用 Sedona 等其他引擎读取。

不过 Esri 在上世纪 90 年代初创建了 Shapefile 格式，因此存在不少局限。

## Shapefile 的限制

Shapefile 的部分缺点包括：

* 不支持复杂几何类型
* 不支持 NULL 值
* 会对数字进行四舍五入
* 对 Unicode 支持较差
* 字段名不能太长
* 单文件大小限制为 2GB
* 空间索引相比其他方案较慢
* 无法存储日期时间

更多关于 [Shapefile 局限性的资料](http://switchfromshapefile.org/) 见此页。

由于这些限制，建议考虑其他更现代的格式。

## Shapefile 的替代方案

适合存储几何数据的格式有很多：

* Iceberg
* [GeoParquet](geoparquet-sedona-spark.md)
* FlatGeoBuf
* [GeoPackage](geopackage-sedona-spark.md)
* [GeoJSON](geojson-sedona-spark.md)
* [CSV](csv-geometry-sedona-spark.md)
* GeoTIFF

## 为什么 Sedona 不支持写出 Shapefile

Sedona 不写出 Shapefile 主要有两个原因：

1. 每个 Shapefile 由多个文件组成，对分布式系统来说写出比较困难。
2. Shapefile 单文件 2GB 的硬限制对一些空间数据来说不够用。

## 结论

Shapefile 是仍在许多生产应用中使用的遗留格式。但其限制颇多，除非需要兼容旧系统，否则在现代数据管道中并不是最佳选择。
