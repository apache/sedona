## Quick start

The detailed explanation is here: [Visualize Spatial DataFrame/RDD](../../tutorial/viz.md).

1. Add Sedona-core, Sedona-SQL,Sedona-Viz into your project POM.xml or build.sbt
2. Declare your Spark Session
```Scala
sparkSession = SparkSession.builder().
config("spark.serializer","org.apache.spark.serializer.KryoSerializer").
config("spark.kryo.registrator", "org.apache.sedona.viz.core.Serde.SedonaVizKryoRegistrator").
master("local[*]").appName("mySedonaVizDemo").getOrCreate()
```
3. Add the following lines after your SparkSession declaration:
```Scala
SedonaSQLRegistrator.registerAll(sparkSession)
SedonaVizRegistrator.registerAll(sparkSession)
```

## Regular functions

### ST_Colorize

Introduction: Given the weight of a pixel, return the corresponding color. The weight can be the spatial aggregation of spatial objects or spatial observations such as temperature and humidity.

!!!note
	The color is encoded to an Integer type value in DataFrame. When you print it, it will show some nonsense values. You can just treat them as colors in GeoSparkViz.

Format: `ST_Colorize (weight:Double, maxWeight:Double, mandatory color: string (Optional))`

Since: `v1.0.0`

#### Produce various colors - heat map

This function will normalize the weight according to the max weight among all pixels. Different pixel obtains different color.

Spark SQL example:
```SQL
SELECT pixels.px, ST_Colorize(pixels.weight, 999) AS color
FROM pixels
```

#### Produce uniform colors - scatter plot

If a mandatory color name is put as the third input argument, this function will directly output this color, without considering the weights. In this case, every pixel will possess the same color.

Spark SQL example:
```SQL
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

Format: `ST_EncodeImage (A:image)`

Since: `v1.0.0`

Spark SQL example:
```SQL
SELECT ST_EncodeImage(images.img)
FROM images
```

### ST_Pixelize

Introduction: Convert a geometry to an array of pixels given a resolution

You should use it together with `Lateral View` and `Explode`

Format: `ST_Pixelize (A:geometry, ResolutionX:int, ResolutionY:int, Boundary:geometry)`

Since: `v1.0.0`

Spark SQL example:
```SQL
SELECT ST_Pixelize(shape, 256, 256, (ST_Envelope_Aggr(shape) FROM pointtable))
FROM polygondf
```

### ST_TileName

Introduction: Return the map tile name for a given zoom level. Please refer to [OpenStreetMap ZoomLevel](http://wiki.openstreetmap.org/wiki/Zoom_levels) and [OpenStreetMap tile name](https://wiki.openstreetmap.org/wiki/Slippy_map_tilenames).

!!!note
	Tile name is formatted as a "Z-X-Y" string. Z is zoom level. X is tile coordinate on X axis. Y is tile coordinate on Y axis.

Format: `ST_TileName (A:pixel, ZoomLevel:int)`

Since: `v1.0.0`

Spark SQL example:
```SQL
SELECT ST_TileName(pixels.px, 3)
FROM pixels
```

## Aggregate functions

### ST_Render

Introduction: Given a group of pixels and their colors, return a single Java PNG BufferedImage. The 3rd parameter is optional and it is the zoom level. You should use zoom level when you want to render tiles, instead of a single image.

Format: `ST_Render (A:pixel, B:color, C:Integer - optional zoom level)`

Since: `v1.0.0`

Spark SQL example:
```SQL
SELECT tilename, ST_Render(pixels.px, pixels.color) AS tileimg
FROM pixels
GROUP BY tilename
```