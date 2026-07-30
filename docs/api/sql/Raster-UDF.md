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

## Raster UDFs

A Python UDF can take raster columns as input and return either an ordinary Spark value or a new raster.
Inside the UDF each raster cell arrives as a `SedonaRaster`, so the pixels are available to NumPy, SciPy,
scikit-learn, rasterio, or any other library in your Python environment.

Raster **input** to Python UDFs has been supported since `v1.6.0`. Returning a raster **from** a UDF is
supported since `v1.9.1`.

UDFs are the recommended way to process rasters in Sedona.
[`RS_MapAlgebra`](Raster-map-algebra.md) is deprecated since `v1.9.1` and will be removed in a future
version; [the NDVI example below](#ndvi-as-map-algebra-and-as-a-udf) shows the same calculation in both
forms to help with migrating.

### Reading pixel data

`SedonaRaster` gives you three views of the same pixels:

```python
raster.as_numpy()  # ndarray in CHW order (bands, height, width)
raster.as_numpy_masked()  # regular ndarray, with NODATA pixels replaced by NaN
raster.as_rasterio()  # read-only rasterio.DatasetReader; NODATA is not attached
```

Metadata is available as attributes — `raster.width`, `raster.height`, `raster.crs_wkt`,
`raster.affine_trans`, and `raster.bands_meta`. The canonical NODATA declaration for Python band index
`i` is `raster.bands_meta[i].nodata`. Index `0` refers to channel `0` in either NumPy array and to band
`1` in rasterio and Sedona SQL functions. A metadata value of `NaN` means that the band has no declared
NODATA.

The returned views do not all retain that declaration:

| Accessor | NODATA behavior |
| --- | --- |
| `as_numpy()` | The ndarray has no NODATA metadata. Pixels keep their raw sentinel values; read the declaration from `raster.bands_meta`. |
| `as_numpy_masked()` | This is a regular ndarray, not a `numpy.ma.MaskedArray`. NODATA pixels become `NaN`, but the original sentinel is not attached to the result; integer data may therefore be promoted to a floating dtype. |
| `as_rasterio()` | The reader does not carry Sedona's NODATA metadata: `src.nodata` is `None` and `src.read_masks()` reports every pixel as valid. Keep using `raster.bands_meta`, and pass a value or mask to rasterio explicitly. |

!!!warning
    `as_numpy()` returns NODATA pixels as their raw sentinel values, so arithmetic and comparisons treat
    holes as ordinary numbers — a threshold like `band < 1400` happily classifies a `-9999` hole as land.
    Whenever the input may carry NODATA, read through `as_numpy_masked()` and re-mark the holes on the way
    out; [Two rasters](#two-rasters) shows the pattern. Examples on this page that read `as_numpy()` assume a
    hole-free input.

### Raster to scalar

Any Spark return type works. Declare it on the `@udf` decorator and apply the UDF like any other function:

```python
from pyspark.sql.functions import col, udf


@udf(returnType="double")
def mean_udf(raster):
    return float(raster.as_numpy().mean())


df.select(mean_udf(col("rast")).alias("mean"))
```

The same UDF can be registered by name for use from SQL. Pass the decorated UDF on its own — the return type
is already attached:

```python
sedona.udf.register("mean_udf", mean_udf)
sedona.sql("SELECT mean_udf(rast) AS mean FROM raster_table")
```

### Raster to raster

To return a raster, declare `RasterType()` as the return type and build the result with
`SedonaRaster.with_bands()`. It takes a NumPy array of new pixel values and carries over the source raster's
CRS, affine transform, and other spatial metadata:

```python
import numpy as np
from pyspark.sql.functions import col, udf

from sedona.spark.sql.types import RasterType


@udf(returnType=RasterType())
def mask_udf(raster):
    band1 = raster.as_numpy()[0]
    mask = (band1 < 1400).astype(np.float32)
    return raster.with_bands(mask)


df.select(mask_udf(col("rast")).alias("mask_rast"))
```

`with_bands()` accepts CHW order (bands × height × width), or HW order (height × width) as shorthand for a
single-band result. The band count and the dtype may both differ from the input — the example above turns a
multi-band scene into one `float32` band. The returned `SedonaRaster` is serialized back to the JVM
automatically, so the output column is an ordinary raster column that every `RS_` function accepts.

#### Setting NODATA on the output

By default each output band inherits NODATA from the input band in the same position, and bands beyond the
input's band count inherit it from the input's last band. That is usually wrong for a derived raster, because
the output means something different from the scene it came from: a 0/1 mask built from a band whose NODATA is
`0` would have every unset pixel treated as NODATA by [`RS_ZonalStats`](Raster-Band-Accessors/RS_ZonalStats.md),
[`RS_Count`](Raster-Band-Accessors/RS_Count.md), and every other function that respects it.

Pass `nodata=` to say what the output means. This is the equivalent of `RS_MapAlgebra`'s `noDataValue`
argument:

```python
NODATA = -9999.0


@udf(returnType=RasterType())
def mask_udf(raster):
    band1 = raster.as_numpy_masked()[0]  # NaN where the input is NODATA
    mask = (band1 < 1400).astype(np.float32)
    return raster.with_bands(np.where(np.isnan(band1), NODATA, mask), nodata=NODATA)
```

A scalar applies to every output band; pass a sequence to set them individually, one entry per band. Use
`float("nan")` for a band that should have no NODATA at all.

```python
return raster.with_bands(stacked, nodata=[-9999.0, float("nan")])
```

If the output dtype cannot represent the inherited value — say a float64 scene with NODATA `-9999` narrowed
to `uint8` — `with_bands()` raises rather than producing metadata that no pixel of the output can ever match.
Pass `nodata=` explicitly in that case, or clean the holes out of the pixel data before casting.

!!!note
    Support for `nodata=` is new in `v1.9.1`. Before that the value was always inherited and had to be
    corrected afterwards with
    [`RS_SetBandNoDataValue`](Raster-Operators/RS_SetBandNoDataValue.md), which also still works.

### NDVI, as map algebra and as a UDF

The same computation written both ways. With `RS_MapAlgebra`:

```sql
SELECT RS_MapAlgebra(rast, 'D', 'out = (rast[3] - rast[0]) / (rast[3] + rast[0]);') AS ndvi
FROM raster_table
```

As a UDF:

```python
@udf(returnType=RasterType())
def ndvi(raster):
    # Reads raw values, exactly like the Jiffle script above. If the scene carries
    # NODATA, use as_numpy_masked() and re-mark the holes — see "Two rasters" below.
    a = raster.as_numpy().astype(np.float64)
    red, nir = a[0], a[3]
    return raster.with_bands((nir - red) / (nir + red + 1e-10))


df.select(ndvi(col("rast")).alias("ndvi"))
```

Both produce a single-band `double` raster on the input's grid. Note that band indexing differs: Jiffle's
`rast[0]` and NumPy's `a[0]` are both the first band, but the SQL functions that consume the result
(`RS_BandAsArray`, `RS_BandNoDataValue`, …) number bands from 1.

### Two rasters

A UDF takes as many raster columns as you need, which covers what the five-argument form of
`RS_MapAlgebra` does. Whichever raster you call `with_bands()` on donates the metadata, so pick the one whose
grid the result belongs to:

```python
NODATA = -9999.0


@udf(returnType=RasterType())
def delta(after, before):
    # as_numpy_masked() substitutes NaN for NODATA, so invalid pixels stay invalid
    # through the arithmetic instead of contributing their sentinel value.
    diff = after.as_numpy_masked()[0] - before.as_numpy_masked()[0]
    return after.with_bands(np.where(np.isnan(diff), NODATA, diff), nodata=NODATA)


df.select(delta(col("after"), col("before")).alias("delta"))
```

!!!warning
    Use [`as_numpy_masked()`](#reading-pixel-data), not `as_numpy()`, whenever a raster that may carry NODATA
    feeds arithmetic or a comparison — with one input or several. `as_numpy()` hands back the raw NODATA
    sentinels, so a hole in one input becomes a large bogus difference, and a hole in *both* inputs cancels
    out into a plausible zero. `nodata=` only
    labels the output — it does not mark which pixels are invalid, so the sentinel has to be written into the
    array as well, as `np.where` does above.

Both rasters must already be on the same grid — see [Limits](#limits). Use
[`RS_ReprojectMatch`](Raster-Operators/RS_ReprojectMatch.md) beforehand if they aren't.

### Using rasterio inside a UDF

`as_rasterio()` hands you a `rasterio.DatasetReader` backed by the same pixel buffer, so rasterio and GDAL
algorithms work on raster columns directly. The dataset is read-only; to return a raster, pass the resulting
array back through `with_bands()`:

```python
import rasterio.fill


@udf(returnType=RasterType())
def fill_udf(raster):
    # NODATA has to come from the SedonaRaster, not from the GDAL dataset — see the note below.
    nodata = raster.bands_meta[0].nodata
    valid = ~np.isnan(raster.as_numpy_masked()[0])
    with raster.as_rasterio() as src:
        filled = rasterio.fill.fillnodata(src.read(1), mask=valid.astype(np.uint8))
    # fillnodata only interpolates within max_search_distance (100 pixels by default)
    # of valid data — cells deeper inside a hole keep their sentinel value. Keeping
    # the NODATA declaration leaves those cells invalid. Only switch to
    # nodata=float("nan") when every hole is small enough to be filled completely.
    return raster.with_bands(filled, nodata=nodata)


df.select(fill_udf(col("rast")).alias("filled"))
```

!!!warning
    The dataset `as_rasterio()` returns does **not** carry the raster's NODATA value. `src.nodata` is always
    `None` and `src.read_masks()` reports every pixel as valid, so any rasterio call that decides what to do
    from the dataset's own NODATA will silently treat holes as data. Take the value from the `SedonaRaster`
    instead — `raster.bands_meta[i].nodata`, or `raster.as_numpy_masked()` which substitutes `NaN` — and pass
    it to the rasterio call explicitly, as the `mask=` argument does above.

This pattern works for any rasterio operation that keeps the grid unchanged — `fillnodata`, `sieve`,
rasterizing onto the existing grid. Operations that change the CRS, resolution, or extent cannot be returned;
see [Limits](#limits).

### Limits

#### The output must sit on the input's grid

`with_bands()` requires the new array to have the same height and width as the source, and it reuses the
source's CRS and affine transform. Handing it a differently shaped array raises:

```
ValueError: Spatial dimensions (2, 2) don't match raster (3, 4)
```

There is no way to return a raster with a different CRS, cell size, or extent, so reprojection, warping,
resampling, and cropping to a new extent cannot be done inside a Python UDF. Use
[`RS_Resample`](Raster-Operators/RS_Resample.md), [`RS_ReprojectMatch`](Raster-Operators/RS_ReprojectMatch.md),
or [`RS_Clip`](Raster-Operators/RS_Clip.md) before or after the UDF instead, or write the UDF
[in Scala](#scala-and-java), where this restriction does not apply.

`RS_MapAlgebra` is grid-preserving in the same way, so this is not a difference between the two.

#### Not every NumPy dtype survives

The array you hand to `with_bands()` is mapped onto a Java data buffer type, and three cases need care:

| NumPy dtype | Result |
|---|---|
| `uint8`, `int16`, `uint16`, `int32`, `float32`, `float64` | mapped directly |
| `uint32` | stored as signed 32-bit; values above 2<sup>31</sup>−1 overflow silently |
| `int8` | stored as unsigned byte; negative values are reinterpreted — `-2` reads back as `254` |
| `int64`, `uint64` | rejected with `ValueError` |

Cast to `float64` when in doubt.

`nodata=` values follow the same storage rules: on an `int8` band `nodata=-2` is stored — and reported by
[`RS_BandNoDataValue`](Raster-Band-Accessors/RS_BandNoDataValue.md) — as `254`, matching the pixels; on a
`uint32` band a value above 2<sup>31</sup>−1 is reported as its signed reinterpretation; on a `float32` band
the value is rounded to the nearest float32, since that is what the pixels themselves hold.

### Scala and Java

Rasters are represented as GeoTools `GridCoverage2D` on the JVM, and `RasterUDT` is registered for that
class, so a Scala UDF can take and return raster columns without any extra setup:

```scala
import org.apache.spark.sql.functions.{col, udf}
import org.geotools.coverage.grid.GridCoverage2D

// Raster to scalar
val numBands = udf((raster: GridCoverage2D) => raster.getNumSampleDimensions)
df.select(numBands(col("rast")).alias("num_bands"))

// Raster to raster
val process = udf((raster: GridCoverage2D) => transform(raster))
df.select(process(col("rast")).alias("rast"))
```

The convenience layer described above — `as_numpy()`, `with_bands()`, and the metadata accessors — is
Python-only. From Scala you work against the GeoTools API directly.

That also means the two limits above do not apply. A Scala UDF constructs the `GridCoverage2D` it returns, so
it controls the band count, CRS, cell size, extent, and NODATA outright — it can take a 4×3 raster in
EPSG:3857 and hand back a 7×5 raster in EPSG:4326. If you need a UDF that changes the grid, Scala is the way
to write it today.

### Further reading

* [Map algebra](Raster-map-algebra.md) — the `RS_MapAlgebra` alternative and its Jiffle script syntax
* [Raster DataFrames in Python](../../tutorial/raster.md#working-with-raster-dataframes-in-python) — collecting rasters to the driver
* [Raster functions](Raster-Functions.md) — the full `RS_` operator surface
