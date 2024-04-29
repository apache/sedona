## RS_Union_Aggr

Introduction: This function combines multiple rasters into a single multiband raster by stacking the bands of each input raster sequentially. The function arranges the bands in the output raster according to the order specified by the index column in the input. It is typically used in scenarios where rasters are grouped by certain criteria (e.g., time and/or location) and an aggregated raster output is desired.

!!!Note
    RS_Union_Aggr expects the following input, if not satisfied then will throw an IllegalArgumentException:

    - Indexes to be in an arithmetic sequence without any gaps.
    - Indexes to be unique and not repeated.
    - Rasters should be of the same shape.

Format: `RS_Union_Aggr(A: rasterColumn, B: indexColumn)`

Since: `v1.5.1`

SQL Example:

First, define a window specification that partitions by geographic location and orders by time. This will prepare your data by assigning an index to each raster based on its timestamp within each location group.
```
val windowSpec = Window.partitionBy("geometry").orderBy("timestamp")
val indexedRasters = df.withColumn("index", row_number().over(windowSpec))

indexedRasters.show()
```

The indexed rasters will appear as follows, showing that each raster is tagged with a sequential index (ordered by timestamp) within its group (geometry).
```
+-------------------+------------------------------+--------------+-----+
|          timestamp|                        raster|      geometry|index|
+-------------------+------------------------------+--------------+-----+
|2021-01-01T00:00:00|GridCoverage2D["geotiff_cov...|POINT (72 120)|    1|
|2021-01-02T00:00:00|GridCoverage2D["geotiff_cov...|POINT (72 120)|    2|
|2021-01-03T00:00:00|GridCoverage2D["geotiff_cov...|POINT (72 120)|    3|
|2021-01-04T00:00:00|GridCoverage2D["geotiff_cov...|POINT (72 120)|    4|
|2021-01-05T00:00:00|GridCoverage2D["geotiff_cov...|POINT (72 120)|    5|
|2021-01-02T00:00:00|GridCoverage2D["geotiff_cov...|POINT (84 132)|    1|
|2021-01-03T00:00:00|GridCoverage2D["geotiff_cov...|POINT (84 132)|    2|
|2021-01-04T00:00:00|GridCoverage2D["geotiff_cov...|POINT (84 132)|    3|
|2021-01-05T00:00:00|GridCoverage2D["geotiff_cov...|POINT (84 132)|    4|
|2021-01-06T00:00:00|GridCoverage2D["geotiff_cov...|POINT (84 132)|    5|
|2021-01-07T00:00:00|GridCoverage2D["geotiff_cov...|POINT (84 132)|    6|
+-------------------+------------------------------+--------------+-----+
```

To create a stacked raster by grouping on geometry.
```
SELECT geometry, RS_Union_Aggr(raster, index) AS raster, RS_NumBands(raster) AS Num_Bands
FROM indexedRasters
WHERE index <= 4
GROUP BY geometry
```

Output:

The query yields rasters grouped by geometry, each containing the first four time steps combined into a single multiband raster, where each band represents one time step.

```
+--------------+--------------------+---------+
|      geometry|              raster|Num_Bands|
+--------------+--------------------+---------+
|POINT (72 120)|GridCoverage2D["g...|        4|
|POINT (84 132)|GridCoverage2D["g...|        4|
+--------------+--------------------+---------+
```
