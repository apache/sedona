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

## 快速入门

详细说明请参见：[可视化空间 DataFrame/RDD](../../tutorial/viz.md)。

1. 在你的项目 pom.xml 或 build.sbt 中加入 Sedona-core、Sedona-SQL、Sedona-Viz
2. 声明你的 Spark Session

```scala
sparkSession = SparkSession.builder().
config("spark.serializer","org.apache.spark.serializer.KryoSerializer").
config("spark.kryo.registrator", "org.apache.sedona.viz.core.Serde.SedonaVizKryoRegistrator").
master("local[*]").appName("mySedonaVizDemo").getOrCreate()
```

3. 在 SparkSession 声明之后加上以下代码：

```scala
SedonaSQLRegistrator.registerAll(sparkSession)
SedonaVizRegistrator.registerAll(sparkSession)
```

## 常规函数

### ST_Colorize

简介：根据像素的权重返回对应的颜色。权重可以是空间对象的空间聚合值，也可以是诸如温度、湿度等空间观测值。

!!!note
	该颜色在 DataFrame 中被编码为 Integer 类型的数值。当你打印它时，看到的会是一些看似无意义的数字。可以直接把它们当作 GeoSparkViz 中的颜色处理。

格式：

```
ST_Colorize (weight: Double, maxWeight: Double, mandatory color: String (Optional))
```

起始版本：`v1.0.0`

#### 生成不同颜色 —— 热力图

该函数会根据所有像素中的最大权重对当前权重做归一化，从而让不同像素得到不同的颜色。

SQL 示例

```sql
SELECT pixels.px, ST_Colorize(pixels.weight, 999) AS color
FROM pixels
```

#### 生成统一颜色 —— 散点图

如果将一个强制颜色名作为第三个输入参数传入，该函数会直接输出该颜色，而忽略权重。这种情况下，所有像素都使用同一个颜色。

SQL 示例

```sql
SELECT pixels.px, ST_Colorize(pixels.weight, 999, 'red') AS color
FROM pixels
```

下面是一些可输入的颜色名示例：

```
"firebrick"
"#aa38e0"
"0x40A8CC"
"rgba(112,36,228,0.9)"
```

预定义颜色的完整列表请参见 [AWT Colors](https://static.javadoc.io/org.beryx/awt-color-factory/1.0.0/org/beryx/awt/color/ColorFactory.html)。

### ST_EncodeImage

简介：返回一个 Java PNG BufferedImage 的 base64 字符串表示。该函数主要用于服务端—客户端环境，例如将 base64 字符串从 GeoSparkViz 传输到 Apache Zeppelin。

格式：`ST_EncodeImage (A: Image)`

起始版本：`v1.0.0`

SQL 示例

```sql
SELECT ST_EncodeImage(images.img)
FROM images
```

### ST_Pixelize

简介：在给定的分辨率下，将一个几何对象转换成像素数组

你应当与 `Lateral View` 和 `Explode` 一起使用该函数。

格式：

```
ST_Pixelize (A: Geometry, ResolutionX: Integer, ResolutionY: Integer, Boundary: Geometry)
```

起始版本：`v1.0.0`

SQL 示例

```sql
SELECT ST_Pixelize(shape, 256, 256, (ST_Envelope_Aggr(shape) FROM pointtable))
FROM polygondf
```

### ST_TileName

简介：返回给定缩放级别下的地图瓦片名称。请参阅 [OpenStreetMap ZoomLevel](http://wiki.openstreetmap.org/wiki/Zoom_levels) 和 [OpenStreetMap tile name](https://wiki.openstreetmap.org/wiki/Slippy_map_tilenames)。

!!!note
	瓦片名称的格式为 "Z-X-Y" 字符串。Z 为缩放级别，X 为 X 轴上的瓦片坐标，Y 为 Y 轴上的瓦片坐标。

格式：`ST_TileName (A: Pixel, ZoomLevel: Integer)`

起始版本：`v1.0.0`

SQL 示例

```sql
SELECT ST_TileName(pixels.px, 3)
FROM pixels
```

## 聚合函数

### ST_Render

简介：给定一组像素及其颜色，返回一个 Java PNG BufferedImage。第 3 个参数为可选的缩放级别。当你需要渲染瓦片而不是单张图像时，应当使用该缩放级别参数。

格式：`ST_Render (A: Pixel, B: Color, C: Integer - optional zoom level)`

起始版本：`v1.0.0`

SQL 示例

```sql
SELECT tilename, ST_Render(pixels.px, pixels.color) AS tileimg
FROM pixels
GROUP BY tilename
```
