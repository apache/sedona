## RS_Union_Aggr

Introduction: Returns a raster containing bands by specified indexes from all rasters in the provided column. Extracts the first bands from each raster and combines them into the output raster based on the input index values.

!!!Note
    RS_Union_Aggr can take multiple banded rasters as input, but it would only extract the first band to the resulting raster. RS_Union_Aggr expects the following input, if not satisfied then will throw an IllegalArgumentException:

    - Indexes to be in an arithmetic sequence without any gaps.
    - Indexes to be unique and not repeated.
    - Rasters should be of the same shape.

Format: `RS_Union_Aggr(A: rasterColumn, B: indexColumn)`

Since: `v1.5.1`

SQL Example

Contents of `raster_table`.

```
+------------------------------+-----+
|                        raster|index|
+------------------------------+-----+
|GridCoverage2D["geotiff_cov...|    1|
|GridCoverage2D["geotiff_cov...|    2|
|GridCoverage2D["geotiff_cov...|    3|
|GridCoverage2D["geotiff_cov...|    4|
|GridCoverage2D["geotiff_cov...|    5|
+------------------------------+-----+
```

```
SELECT RS_Union_Aggr(raster, index) FROM raster_table
```

Output:

This output raster contains the first band of each raster in the `raster_table` at specified index.

```
GridCoverage2D["geotiff_coverage", GeneralEnvel...
```
