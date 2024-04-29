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

First, we enrich the dataset with time-based grouping columns and index the rasters based on time intervals:

```
// Add yearly and quarterly time interval columns for grouping
df = df
 .withColumn("year", year($"timestamp"))
 .withColumn("quarter", quarter($"timestamp"))

// Define window specs for quarterly indexing within each geometry-year group
windowSpecQuarter = Window.partitionBy("geometry", "year", "quarter").orderBy("timestamp")

indexedDf = df.withColumn("index", row_number().over(windowSpecQuarter))

indexedDf.show()
```

The indexed rasters will appear as follows, showing that each raster is tagged with a sequential index (ordered by timestamp) within its group (grouped by geometry, year and quarter).

```
+-------------------+-----------------------------+--------------+----+-------+-----+
|timestamp          |raster                       |geometry      |year|quarter|index|
+-------------------+-----------------------------+--------------+----+-------+-----+
|2021-01-10 00:00:00|GridCoverage2D["geotiff_co...|POINT (72 120)|2021|1      |1    |
|2021-01-25 00:00:00|GridCoverage2D["geotiff_co...|POINT (72 120)|2021|1      |2    |
|2021-02-15 00:00:00|GridCoverage2D["geotiff_co...|POINT (72 120)|2021|1      |3    |
|2021-03-15 00:00:00|GridCoverage2D["geotiff_co...|POINT (72 120)|2021|1      |4    |
|2021-03-25 00:00:00|GridCoverage2D["geotiff_co...|POINT (72 120)|2021|1      |5    |
|2021-04-10 00:00:00|GridCoverage2D["geotiff_co...|POINT (84 132)|2021|2      |1    |
|2021-04-22 00:00:00|GridCoverage2D["geotiff_co...|POINT (84 132)|2021|2      |2    |
|2021-05-15 00:00:00|GridCoverage2D["geotiff_co...|POINT (84 132)|2021|2      |3    |
|2021-05-20 00:00:00|GridCoverage2D["geotiff_co...|POINT (84 132)|2021|2      |4    |
|2021-05-29 00:00:00|GridCoverage2D["geotiff_co...|POINT (84 132)|2021|2      |5    |
|2021-06-10 00:00:00|GridCoverage2D["geotiff_co...|POINT (84 132)|2021|2      |6    |
+-------------------+-----------------------------+------------- +----+-------+-----+
```

To create a stacked raster by grouping on geometry.

```
indexedDf.createOrReplaceTempView("indexedDf")

sedona.sql('''
    SELECT geometry, year, quarter, RS_Union_Aggr(raster, index) AS aggregated_raster
    FROM indexedDf
    WHERE index <= 4
    GROUP BY geometry, year, quarter
''').show()
```

Output:

The query yields rasters grouped by geometry, year and quarter, each containing the first four time steps combined into a single multiband raster, where each band represents one time step.

```
+--------------+----+-------+--------------------+---------+
|      geometry|year|quarter|              raster|Num_Bands|
+--------------+----+-------+--------------------+---------+
|POINT (72 120)|2021|1      |GridCoverage2D["g...|        4|
|POINT (84 132)|2021|2      |GridCoverage2D["g...|        4|
+--------------+----+-------+--------------------+---------+
```
