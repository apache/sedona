## RS_Union_Aggr

Introduction: Returns a raster containing bands by specified indexes from all rasters in the provided column. Extracts bands from each raster based on the input index values and combines them into the output.

!!!Note
    RS_Union_Aggr expects the following input, if not satisfied then will throw an IllegalArgumentException:
    
    - Indexes to be in an arithmetic sequence without any gaps.
    - Indexes to be unique and not repeated.
    - Rasters should be of the same shape.

Format: `RS_Union_Aggr(A: rasterColumn, B: indexColumn)`

Since: `v1.5.1`

Spark SQL Example:

```
SELECT RS_Union_Aggr(rasters, index)
```

Output:

```
GridCoverage2D["geotiff_coverage", GeneralEnvel...
```
