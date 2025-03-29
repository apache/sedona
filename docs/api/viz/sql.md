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

## Quick start

The detailed explanation is here: [Visualize Spatial DataFrame/RDD](../../tutorial/viz.md).

1. Add Sedona-core, Sedona-SQL, Sedona-Viz into your project pom.xml or build.sbt
2. Declare your Spark Session

```scala
sparkSession = SparkSession.builder().
config("spark.serializer","org.apache.spark.serializer.KryoSerializer").
config("spark.kryo.registrator", "org.apache.sedona.viz.core.Serde.SedonaVizKryoRegistrator").
master("local[*]").appName("mySedonaVizDemo").getOrCreate()
```

3. Add the following lines after your SparkSession declaration:

```scala
SedonaSQLRegistrator.registerAll(sparkSession)
SedonaVizRegistrator.registerAll(sparkSession)
```

## Regular functions

### ST_Colorize

Introduction: Given the weight of a pixel, return the corresponding color. The weight can be the spatial aggregation of spatial objects or spatial observations such as temperature and humidity.

!!!note
	The color is encoded to an Integer type value in DataFrame. When you print it, it will show some nonsense values. You can just treat them as colors in GeoSparkViz.

Format:

```
ST_Colorize (weight: Double, maxWeight: Double, mandatory color: String (Optional))
```

Since: `v1.0.0`

#### Produce various colors - heat map

This function will normalize the weight according to the max weight among all pixels. Different pixel obtains different color.

SQL Example

```sql
SELECT pixels.px, ST_Colorize(pixels.weight, 999) AS color
FROM pixels
```

#### Produce uniform colors - scatter plot

If a mandatory color name is put as the third input argument, this function will directly output this color, without considering the weights. In this case, every pixel will possess the same color.

SQL Example

```sql
SELECT pixels.px, ST_Colorize(pixels.weight, 999, 'red') AS color
FROM pixels
```

Here are some example color names can be entered:

```
"firebrick"
"#aa38e0"
"0x40A8CC"
"rgba(112,36,228,0.9)"
```

Please refer to [AWT Colors](https://static.javadoc.io/org.beryx/awt-color-factory/1.0.0/org/beryx/awt/color/ColorFactory.html) for a list of pre-defined colors.

### ST_EncodeImage

Introduction: Return the base64 string representation of a Java PNG BufferedImage. This is specific for the server-client environment. For example, transfer the base64 string from GeoSparkViz to Apache Zeppelin.

Format: `ST_EncodeImage (A: Image)`

Since: `v1.0.0`

SQL Example

```sql
SELECT ST_EncodeImage(images.img)
FROM images
```

### ST_Pixelize

Introduction: Convert a geometry to an array of pixels given a resolution

You should use it together with `Lateral View` and `Explode`

Format:

```
ST_Pixelize (A: Geometry, ResolutionX: Integer, ResolutionY: Integer, Boundary: Geometry)
```

Since: `v1.0.0`

SQL Example

```sql
SELECT ST_Pixelize(shape, 256, 256, (ST_Envelope_Aggr(shape) FROM pointtable))
FROM polygondf
```

### ST_TileName

Introduction: Return the map tile name for a given zoom level. Please refer to [OpenStreetMap ZoomLevel](http://wiki.openstreetmap.org/wiki/Zoom_levels) and [OpenStreetMap tile name](https://wiki.openstreetmap.org/wiki/Slippy_map_tilenames).

!!!note
	Tile name is formatted as a "Z-X-Y" string. Z is zoom level. X is tile coordinate on X axis. Y is tile coordinate on Y axis.

Format: `ST_TileName (A: Pixel, ZoomLevel: Integer)`

Since: `v1.0.0`

SQL Example

```sql
SELECT ST_TileName(pixels.px, 3)
FROM pixels
```

## Aggregate functions

### ST_Render

Introduction: Given a group of pixels and their colors, return a single Java PNG BufferedImage. The 3rd parameter is optional and it is the zoom level. You should use zoom level when you want to render tiles, instead of a single image.

Format: `ST_Render (A: Pixel, B: Color, C: Integer - optional zoom level)`

Since: `v1.0.0`

SQL Example

```sql
SELECT tilename, ST_Render(pixels.px, pixels.color) AS tileimg
FROM pixels
GROUP BY tilename
```
