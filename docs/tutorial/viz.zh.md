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

本页介绍如何使用 SedonaViz 可视化空间数据。==示例代码使用 Scala 编写，但同样适用于 Java==。

SedonaViz 通过对 Sedona 处理大规模空间数据的能力进行扩展，原生支持通用的地图制图设计。它可以可视化空间 RDD 与空间查询，并以并行方式渲染超高分辨率图像。

SedonaViz 提供了 Map Visualization SQL，使用户能更灵活地设计美观的地图可视化效果，包括散点图与热力图。同时也提供 SedonaViz RDD API。

!!!note
	SedonaViz 的 SQL/DataFrame API 全部说明请参阅 [SedonaViz API](../api/viz/sql.md)。

## 为什么需要可扩展的地图可视化

数据可视化让用户能够对数据进行总结、分析与推理。要在多个缩放级别上保证细致而准确的地理空间地图可视化，需要极高分辨率的地图。Google Maps、MapBox、ArcGIS 等传统方案受计算资源限制，对大规模地理空间数据生成地图需要花费大量时间。在大空间数据场景下，这类工具往往直接崩溃或长时间无法完成。

SedonaViz 把地图可视化流程的主要步骤（如像素化、聚合、渲染）封装为一组可大规模并行的 GeoViz 算子，用户可以自由组合任意自定义样式。

## 可视化 SpatialRDD

本教程主要介绍 SQL/DataFrame API。

## 配置依赖

1. 阅读 [Sedona Maven Central 坐标](../setup/maven-coordinates.md)
2. 添加 [Apache Spark core](https://mvnrepository.com/artifact/org.apache.spark/spark-core_2.11)、[Apache SparkSQL](https://mvnrepository.com/artifact/org.apache.spark/spark-sql)、Sedona-core、Sedona-SQL、Sedona-Viz 依赖

## 创建 Sedona 配置

在程序开头使用以下代码创建 Sedona 配置。如果您已经有了由 Wherobots / AWS EMR / Databricks 创建的 SparkSession（通常名为 `spark`），可跳过此步骤直接使用 `spark`。

==Sedona >= 1.4.1===

```scala
val config = SedonaContext.builder()
		.config("spark.kryo.registrator", classOf[SedonaVizKryoRegistrator].getName) // org.apache.sedona.viz.core.Serde.SedonaVizKryoRegistrator
		.master("local[*]") // 集群模式下请删除此行
		.appName("Sedona Viz") // 改成合适的名字
		.getOrCreate()
```

==Sedona <1.4.1==

下面这种方式自 Sedona 1.4.1 起已弃用，请改用上面的方式。

```scala
var sparkSession = SparkSession.builder()
.master("local[*]") // 集群模式下请删除此行
.appName("Sedona Viz") // 改成合适的名字
// 启用 Sedona 自定义 Kryo 序列化器
.config("spark.serializer", classOf[KryoSerializer].getName) // org.apache.spark.serializer.KryoSerializer
.config("spark.kryo.registrator", classOf[SedonaVizKryoRegistrator].getName) // org.apache.sedona.viz.core.Serde.SedonaVizKryoRegistrator
.getOrCreate()
```

## 初始化 SedonaContext

在创建 Sedona 配置之后加上以下代码。如果您已经有了由 Wherobots / AWS EMR / Databricks 创建的 SparkSession（通常名为 `spark`），请改为调用 `SedonaContext.create(spark)`。

==Sedona >= 1.4.1===

```scala
val sedona = SedonaContext.create(config)
SedonaVizRegistrator.registerAll(sedona)
```

==Sedona <1.4.1==

下面这种方式自 Sedona 1.4.1 起已弃用，请改用上面的方式创建 SedonaContext。

```scala
SedonaSQLRegistrator.registerAll(sparkSession)
SedonaVizRegistrator.registerAll(sparkSession)
```

也可以通过在 `spark-submit` 或 `spark-shell` 中传入 `--conf spark.sql.extensions=org.apache.sedona.viz.sql.SedonaVizExtensions,org.apache.sedona.sql.SedonaSqlExtensions` 来一并注册。

## 创建空间 DataFrame

假设有如下 DataFrame：

```
+----------+---------+
|       _c0|      _c1|
+----------+---------+
|-88.331492|32.324142|
|-88.175933|32.360763|
|-88.388954|32.357073|
|-88.221102| 32.35078|
```

首先需要构造一列几何类型的列：

```sql
CREATE OR REPLACE TEMP VIEW pointtable AS
SELECT ST_Point(cast(pointtable._c0 as Decimal(24,20)),cast(pointtable._c1 as Decimal(24,20))) as shape
FROM pointtable
```

Sedona 提供了多种方式加载各种空间数据格式，详见 [编写空间 DataFrame 应用](sql.md)。

## 生成单张图像

大多数情况下，您只想从空间数据中得到一张图像。

### 像素化空间对象

要在地图图像上显示空间对象，先要把它们转换为像素。

首先计算该列的空间边界：

```sql
CREATE OR REPLACE TEMP VIEW boundtable AS
SELECT ST_Envelope_Aggr(shape) as bound FROM pointtable
```

然后用 ST_Pixelize 将其转换为像素。

下面这段代码适用于 Sedona v1.0.1 之前。`ST_Pixelize` 继承自 Generator，因此可以直接展开数组而无需 **explode** 函数。

```sql
CREATE OR REPLACE TEMP VIEW pixels AS
SELECT pixel, shape FROM pointtable
LATERAL VIEW ST_Pixelize(ST_Transform(shape, 'epsg:4326','epsg:3857'), 256, 256, (SELECT ST_Transform(bound, 'epsg:4326','epsg:3857') FROM boundtable)) AS pixel
```

下面这段代码适用于 Sedona v1.0.1 及之后。`ST_Pixelize` 返回像素数组，需要使用 **explode** 展开：

```sql
CREATE OR REPLACE TEMP VIEW pixels AS
SELECT pixel, shape FROM pointtable
LATERAL VIEW explode(ST_Pixelize(ST_Transform(shape, 'epsg:4326','epsg:3857'), 256, 256, (SELECT ST_Transform(bound, 'epsg:4326','epsg:3857') FROM boundtable))) AS pixel
```

执行完本教程末尾的 `ST_Render` 后，将得到 256*256 分辨率的图像。

!!!warning
	强烈建议先用 `ST_Transform` 将坐标转换到适合可视化的坐标系（如 epsg:3857），否则地图可能出现变形。

### 聚合像素

许多对象可能被像素化到同一个像素位置。需要按空间聚合或按温度、湿度等空间观测值进行聚合：

```sql
CREATE OR REPLACE TEMP VIEW pixelaggregates AS
SELECT pixel, count(*) as weight
FROM pixels
GROUP BY pixel
```

`weight` 表示空间聚合或空间观测的程度，后续会决定该像素的颜色。

### 给像素上色

运行以下命令为像素根据 weight 上色：

```sql
CREATE OR REPLACE TEMP VIEW pixelaggregates AS
SELECT pixel, ST_Colorize(weight, (SELECT max(weight) FROM pixelaggregates)) as color
FROM pixelaggregates
```

详细 API 说明请参阅 [ST_Colorize](../api/viz/sql.md#st_colorize)。

### 渲染图像

使用 `ST_Render` 把所有像素绘制到一张图像上：

```sql
CREATE OR REPLACE TEMP VIEW images AS
SELECT ST_Render(pixel, color) AS image, (SELECT ST_AsText(bound) FROM boundtable) AS boundary
FROM pixelaggregates
```

该 DataFrame 中将包含一列 Image 类型的列，且只有一张图像。

### 将图像保存到磁盘

从上面的 DataFrame 中取出图像：

```
var image = sedona.table("images").take(1)(0)(0).asInstanceOf[ImageSerializableWrapper].getImage
```

使用 Sedona Viz 的 `ImageGenerator` 把图像保存到磁盘：

```scala
var imageGenerator = new ImageGenerator
imageGenerator.SaveRasterImageAsLocalFile(image, System.getProperty("user.dir")+"/target/points", ImageType.PNG)
```

## 生成地图瓦片

如果您是地图相关从业者，可能需要为不同缩放级别生成地图瓦片，最终构建出地图瓦片图层。

### 像素化与像素聚合

请先用与单张图像生成相同的命令完成像素化与像素聚合。在 `ST_Pixelize` 中需要指定较高的分辨率，例如 1000*1000。注意：每个维度都应能被 2^zoom-level 整除。

### 计算 tile name

使用以下命令为每个像素计算 tile name：

```sql
CREATE OR REPLACE TEMP VIEW pixelaggregates AS
SELECT pixel, weight, ST_TileName(pixel, 3) AS pid
FROM pixelaggregates
```

其中 `3` 表示这些地图瓦片的缩放级别。

### 像素上色

使用与单张图像生成相同的命令进行上色。

### 渲染地图瓦片

把像素按 tile 分组，然后并行渲染各个瓦片图像：

```sql
CREATE OR REPLACE TEMP VIEW images AS
SELECT ST_Render(pixel, color, 3) AS image
FROM pixelaggregates
GROUP BY pid
```

`3` 是这些地图瓦片的缩放级别。

### 将地图瓦片保存到磁盘

可以沿用单张图像生成中的命令，将所有地图瓦片逐一取出并保存。
