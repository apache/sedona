## Map Algebra

Map algebra is a way to perform raster calculations using mathematical expressions. The expression can be a simple arithmetic operation or a complex combination of multiple operations. The expression can be applied to a single raster band or multiple raster bands. The result of the expression is a new raster.

Apache Sedona provides two ways to perform map algebra operations:

1. Using the `RS_MapAlgebra` function.
2. Using `RS_BandAsArray` and array based map algebra functions, such as `RS_Add`, `RS_Multiply`, etc.

Generally, the `RS_MapAlgebra` function is more flexible and can be used to perform more complex operations. The function takes three to four arguments:

```sql
RS_MapAlgebra(rast: Raster, pixelType: String, script: String, [noDataValue: Double])
```

* `rast`: The raster to apply the map algebra expression to.
* `pixelType`: The data type of the output raster. This can be one of `D` (double), `F` (float), `I` (integer), `S` (short), `US` (unsigned short) or `B` (byte). If specified `NULL`, the output raster will have the same data type as the input raster.
* `script`: The map algebra script. [Refer here for more details on the format.](#:~:text=The Jiffle script is,current output pixel value)
* `noDataValue`: (Optional) The nodata value of the output raster.

As of version `v1.5.1`, the `RS_MapAlgebra` function allows two raster column inputs, with multi-band rasters supported. The function accepts 5 parameters:

```sql
RS_MapAlgebra(rast0: Raster, rast1: Raster, pixelType: String, script: String, noDataValue: Double)
```

* `rast0`: The first raster to apply the map algebra expression to.
* `rast1`: The second raster to apply the map algebra expression to.
* `pixelType`: The data type of the output raster. This can be one of `D` (double), `F` (float), `I` (integer), `S` (short), `US` (unsigned short) or `B` (byte). If specified `NULL`, the output raster will have the same data type as the input raster.
* `script`: The map algebra script. [Refer here for more details on the format.](#:~:text=The Jiffle script is,current output pixel value)
* `noDataValue`: (Not optional) The nodata value of the output raster, `null` is allowed.

Spark SQL Example for two raster input `RS_MapAlgebra`:

```sql
RS_MapAlgebra(rast0, rast1, 'D', 'out = rast0[0] * 0.5 + rast1[0] * 0.5;', null)
```

`RS_MapAlgebra` also has good performance, since it is backed by [Jiffle](https://github.com/geosolutions-it/jai-ext/wiki/Jiffle) and can be compiled to Java bytecode for
execution. We'll demonstrate both approaches to implementing commonly used map algebra operations.

!!!Note
    The `RS_MapAlgebra` function can cast the output raster to a different data type specified by `pixelType`:

    - If `pixelType` is smaller than the input raster data type, narrowing casts will be performed, which may result in loss of data.

    - If `pixelType` is larger, widening casts will retain data accuracy.

    - If `pixelType` matches the input raster data type, no casting occurs.

    This allows controlling the output pixel data type. Users should consider potential precision impacts when coercing to a smaller type.

### NDVI

The Normalized Difference Vegetation Index (NDVI) is a simple graphical indicator that can be used to analyze remote sensing measurements, typically, but not necessarily, from a space platform, and assess whether the target being observed contains live green vegetation or not. NDVI has become a de facto standard index used to determine whether a given area contains live green vegetation or not. The NDVI is calculated from these individual measurements as follows:

```
NDVI = (NIR - Red) / (NIR + Red)
```

where NIR is the near-infrared band and Red is the red band.

Assume that we have a bunch of rasters with 4 bands: red, green, blue, and near-infrared. We want to calculate the NDVI for each raster. We can use the `RS_MapAlgebra` function to do this:

```sql
SELECT RS_MapAlgebra(rast, 'D', 'out = (rast[3] - rast[0]) / (rast[3] + rast[0]);') as ndvi FROM raster_table
```

The Jiffle script is `out = (rast[3] - rast[0]) / (rast[3] + rast[0]);`. The `rast` variable is always bound to the input raster, and
the `out` variable is bound to the output raster. Jiffle iterates over all the pixels in the input raster and executes the script for each pixel. the `rast[3]` and `rast[0]`
refers to the current pixel values of the near-infrared and red bands, respectively. The `out` variable is the current output pixel value.

The result of the `RS_MapAlgebra` function is a raster with a single band. The band is of type double, since we specified `D` as the `pixelType` argument.

We can implement the same NDVI calculation using the array based map algebra functions:

```sql
SELECT RS_Divide(
        RS_Subtract(RS_BandAsArray(rast, 1), RS_BandAsArray(rast, 4)),
        RS_Add(RS_BandAsArray(rast, 1), RS_BandAsArray(rast, 4))) as ndvi FROM raster_table
```

The `RS_BandAsArray` function extracts the specified band of the input raster to an array of double, and the `RS_Add`, `RS_Subtract`, and `RS_Divide` functions perform the arithmetic operations on the arrays. The code using the array based map algebra functions is more verbose. However, there is a `RS_NormalizedDifference` function that can be used to calculate the NDVI more concisely:

```sql
SELECT RS_NormalizedDifference(RS_BandAsArray(rast, 1), RS_BandAsArray(rast, 4)) as ndvi FROM raster_table
```

The result of array based map algebra functions is an array of double. User can use `RS_AddBandFromArray` to add the array to a raster as a new band.

### AWEI

The Automated Water Extraction Index (AWEI) is a spectral index that can be used to extract water bodies from remote sensing imagery. The AWEI is calculated from these individual measurements as follows:

```
AWEI = 4 * (Green - SWIR2) - (0.25 * NIR + 2.75 * SWIR1)
```

AWEI can be implemented easily using `RS_MapAlgebra`:

```sql
-- Assume that the raster includes all 13 Sentinel-2 bands
SELECT RS_MapAlgebra(rast, 'D', 'out = 4 * (rast[2] - rast[11]) - (0.25 * rast[7] + 2.75 * rast[12]);') as awei FROM raster_table
```

We can also implement the same AWEI calculation using array based map algebra functions. The code looks more verbose:

```sql
SELECT RS_Subtract(
    RS_Add(RS_MultiplyFactor(band_nir, 0.25), RS_MultiplyFactor(band_swir1, 2.75)),
    RS_MultiplyFactor(RS_Subtract(band_swir2, band_green), 4)) as awei
FROM (
SELECT RS_BandAsArray(rast, 3) AS band_green,
       RS_BandAsArray(rast, 12) AS band_swir2,
       RS_BandAsArray(rast, 13) AS band_swir1,
       RS_BandAsArray(rast, 8) AS band_nir
FROM raster_table) t
```

### Further Reading

* [Jiffle language summary](https://github.com/geosolutions-it/jai-ext/wiki/Jiffle---language-summary)
* [Raster operators](Raster-operators.md)
