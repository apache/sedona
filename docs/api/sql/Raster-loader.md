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

!!!note
	Sedona loader are available in Scala, Java and Python and have the same APIs.

## Loading raster using the raster data source

The `raster` data source loads GeoTiff files and automatically splits them into smaller tiles. Each tile is a row in the resulting DataFrame stored in `Raster` format.

=== "Scala"
    ```scala
    var rawDf = sedona.read.format("raster").load("/some/path/*.tif")
    rawDf.createOrReplaceTempView("rawdf")
    rawDf.show()
    ```

=== "Java"
    ```java
    Dataset<Row> rawDf = sedona.read().format("raster").load("/some/path/*.tif");
    rawDf.createOrReplaceTempView("rawdf");
    rawDf.show();
    ```

=== "Python"
    ```python
    rawDf = sedona.read.format("raster").load("/some/path/*.tif")
    rawDf.createOrReplaceTempView("rawdf")
    rawDf.show()
    ```

The output will look like this:

```
+--------------------+---+---+----+
|                rast|  x|  y|name|
+--------------------+---+---+----+
|GridCoverage2D["g...|  0|  0| ...|
|GridCoverage2D["g...|  1|  0| ...|
|GridCoverage2D["g...|  2|  0| ...|
...
```

The output contains the following columns:

- `rast`: The raster data in `Raster` format.
- `x`: The 0-based x-coordinate of the tile. This column is only present when retile is not disabled.
- `y`: The 0-based y-coordinate of the tile. This column is only present when retile is not disabled.
- `name`: The name of the raster file.

The size of the tile is determined by the internal tiling scheme of the raster data. It is recommended to use [Cloud Optimized GeoTIFF (COG)](https://www.cogeo.org/) format for raster data since they usually organize pixel data as square tiles. You can also disable automatic tiling using `option("retile", "false")`, or specify the tile size manually using options such as `option("tileWidth", "256")` and `option("tileHeight", "256")`.

The options for the `raster` data source are as follows:

- `retile`: Whether to enable tiling. Default is `true`.
- `tileWidth`: The width of the tile. If not specified, the size of internal tiles will be used.
- `tileHeight`: The height of the tile. If not specified, will use `tileWidth` if `tileWidth` is explicitly set, otherwise the size of internal tiles will be used.
- `padWithNoData`: Pad the right and bottom of the tile with NODATA values if the tile is smaller than the specified tile size. Default is `false`.

!!!note
    If the internal tiling scheme of raster data is not friendly for tiling, the `raster` data source will throw an error, and you can disable automatic tiling using `option("retile", "false")`, or specify the tile size manually to workaround this issue. A better solution is to translate the raster data into COG format using `gdal_translate` or other tools.

The `raster` data source also works with Spark generic file source options, such as `option("pathGlobFilter", "*.tif*")` and `option("recursiveFileLookup", "true")`. For instance, you can load all the `.tif` files recursively in a directory using

```python
sedona.read.format("raster").option("recursiveFileLookup", "true").option(
    "pathGlobFilter", "*.tif*"
).load(path_to_raster_data_folder)
```

One difference from other file source loaders is that when the loaded path ends with `/`, the `raster` data source will look up raster files in the directory and all its subdirectories recursively. This is equivalent to specifying a path without trailing `/` and setting `option("recursiveFileLookup", "true")`.

## Loading raster using binaryFile loader (Deprecated)

The raster loader of Sedona leverages Spark built-in binary data source and works with several RS constructors to produce Raster type. Each raster is a row in the resulting DataFrame and stored in a `Raster` format.

!!!tip
    After loading rasters, you can quickly visualize them in a Jupyter notebook using `SedonaUtils.display_image(df)`. It automatically detects raster columns and renders them as images. See [Raster visualizer docs](Raster-Output/index.md) for details.

By default, these functions uses lon/lat order since `v1.5.0`. Before, it used lat/lon order.

### Step 1: Load raster to a binary DataFrame

You can load any type of raster data using the code below. Then use the RS constructors below to create a Raster DataFrame.

```scala
sedona.read.format("binaryFile").load("/some/path/*.asc")
```

### Step 2: Create a raster type column

Use one of the following raster constructors to create a Raster DataFrame:

- [RS_FromArcInfoAsciiGrid](Raster-Constructors/RS_FromArcInfoAsciiGrid.md) - Create raster from Arc Info Ascii Grid files
- [RS_FromGeoTiff](Raster-Constructors/RS_FromGeoTiff.md) - Create raster from GeoTiff files
- [RS_MakeEmptyRaster](Raster-Constructors/RS_MakeEmptyRaster.md) - Create an empty raster geometry

See the full list of [Raster Constructors](Raster-Constructors/index.md) for more options.
