## RS_Union_Aggr

Introduction: This function combines multiple rasters into a single multiband raster by stacking the bands of each input raster sequentially. The function arranges the bands in the output raster according to the order specified by the index column in the input. It is typically used in scenarios where rasters are grouped by certain criteria (e.g., time or location) and an aggregated raster output is desired

!!!Note
    RS_Union_Aggr expects the following input, if not satisfied then will throw an IllegalArgumentException:

    - Indexes to be in an arithmetic sequence without any gaps.
    - Indexes to be unique and not repeated.
    - Rasters should be of the same shape.

Format: `RS_Union_Aggr(A: rasterColumn, B: indexColumn)`

Since: `v1.5.1`

SQL Example

```
val windowSpec = Window.orderBy("timestamp")
val indexedRasters = df.withColumn("index", row_number().over(windowSpec))

indexedRasters.show()
```

```
+-----+-------------------+------------------------------+
|index|          timestamp|                        raster|
+-----+-------------------+------------------------------+
|    1|2021-01-01T00:00:00|GridCoverage2D["geotiff_cov...|
|    2|2021-01-02T00:00:00|GridCoverage2D["geotiff_cov...|
|    3|2021-01-03T00:00:00|GridCoverage2D["geotiff_cov...|
|    4|2021-01-04T00:00:00|GridCoverage2D["geotiff_cov...|
|    5|2021-01-05T00:00:00|GridCoverage2D["geotiff_cov...|
+-----+-------------------+------------------------------+
```

```
SELECT RS_Union_Aggr(raster, index) AS raster, RS_NumBands(raster) AS Num_Bands FROM raster_table
```

Output:

This output raster contains all bands of each raster in the `raster_table`.

```
+--------------------+---------+
|              raster|Num_Bands|
+--------------------+---------+
|GridCoverage2D["g...|        5|
+--------------------+---------+
```
