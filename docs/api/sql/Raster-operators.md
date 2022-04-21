## RS_AddBands

Introduction: Add two spectral bands in a Geotiff image 

Format: `RS_AddBands (Band1: Array[Double], Band2: Array[Double])`

Since: `v1.1.0`

Spark SQL example:
```Scala

val sumDF = spark.sql("select RS_AddBands(band1, band2) as sumOfBands from dataframe")

```

## RS_SubtractBands

Introduction: Subtract two spectral bands in a Geotiff image(band2 - band1)

Format: `RS_SubtractBands (Band1: Array[Double], Band2: Array[Double])`

Since: `v1.1.0`

Spark SQL example:
```Scala

val subtractDF = spark.sql("select RS_SubtractBands(band1, band2) as differenceOfOfBands from dataframe")

```

## RS_MultiplyBands

Introduction: Multiply two spectral bands in a Geotiff image

Format: `RS_MultiplyBands (Band1: Array[Double], Band2: Array[Double])`

Since: `v1.1.0`

Spark SQL example:
```Scala

val multiplyDF = spark.sql("select RS_MultiplyBands(band1, band2) as multiplyBands from dataframe")

```

## RS_DivideBands

Introduction: Divide band1 with band2 from a geotiff image

Format: `RS_DivideBands (Band1: Array[Double], Band2: Array[Double])`

Since: `v1.1.0`

Spark SQL example:
```Scala

val multiplyDF = spark.sql("select RS_DivideBands(band1, band2) as divideBands from dataframe")

```

## RS_MultiplyFactor

Introduction: Multiply a factor to a spectral band in a geotiff image

Format: `RS_MultiplyFactor (Band1: Array[Double], Factor: Int)`

Since: `v1.1.0`

Spark SQL example:
```Scala

val multiplyFactorDF = spark.sql("select RS_MultiplyFactor(band1, 2) as multiplyfactor from dataframe")

```

## RS_Mode

Introduction: Returns Mode from a spectral band in a Geotiff image in form of an array

Format: `RS_Mode (Band: Array[Double])`

Since: `v1.1.0`

Spark SQL example:
```Scala

val modeDF = spark.sql("select RS_Mode(band) as mode from dataframe")

```

## RS_Mean

Introduction: Returns Mean value for a spectral band in a Geotiff image

Format: `RS_Mean (Band: Array[Double])`

Since: `v1.1.0`

Spark SQL example:
```Scala

val meanDF = spark.sql("select RS_Mean(band) as mean from dataframe")

```

## RS_NormalizedDifference

Introduction: Returns Normalized Difference between two bands(band2 and band1) in a Geotiff image(example: NDVI, NDBI) 

Format: `RS_NormalizedDifference (Band1: Array[Double], Band2: Array[Double])`

Since: `v1.1.0`

Spark SQL example:
```Scala

val normalizedDF = spark.sql("select RS_NormalizedDifference(band1, band2) as normdifference from dataframe")

```

## RS_Count

Introduction: Returns count of a particular value from a spectral band in a raster image

Format: `RS_Count (Band1: Array[Double], Target: Double)`

Since: `v1.1.0`

Spark SQL example:
```Scala

val countDF = spark.sql("select RS_Count(band1, target) as count from dataframe")

```

## RS_GreaterThan

Introduction: Mask all the values with 1 which are greater than a particular target value 

Format: `RS_GreaterThan (Band: Array[Double], Target: Double)`

Since: `v1.1.0`

Spark SQL example:
```Scala

val greaterDF = spark.sql("select RS_GreaterThan(band, target) as maskedvalues from dataframe")

```

## RS_GreaterThanEqual

Introduction: Mask all the values with 1 which are greater than equal to a particular target value

Format: `RS_GreaterThanEqual (Band: Array[Double], Target: Double)`

Since: `v1.1.0`

Spark SQL example:
```Scala

val greaterEqualDF = spark.sql("select RS_GreaterThanEqual(band, target) as maskedvalues from dataframe")

```

## RS_LessThan

Introduction: Mask all the values with 1 which are less than a particular target value

Format: `RS_LessThan (Band: Array[Double], Target: Double)`

Since: `v1.1.0`

Spark SQL example:
```Scala

val lessDF = spark.sql("select RS_LessThan(band, target) as maskedvalues from dataframe")

```

## RS_LessThanEqual

Introduction: Mask all the values with 1 which are less than equal to a particular target value

Format: `RS_LessThanEqual (Band: Array[Double], Target: Double)`

Since: `v1.1.0`

Spark SQL example:
```Scala

val lessEqualDF = spark.sql("select RS_LessThanEqual(band, target) as maskedvalues from dataframe")

```

## RS_Modulo

Introduction: Find modulo of pixels with respect to a particular value

Format: `RS_Modulo (Band: Array[Double], Target: Double)`

Since: `v1.1.0`

Spark SQL example:
```Scala

val moduloDF = spark.sql("select RS_Modulo(band, target) as modulo from dataframe")

```

## RS_BitwiseAND

Introduction: Find Bitwise AND between two bands of Geotiff image

Format: `RS_BitwiseAND (Band1: Array[Double], Band2: Array[Double])`

Since: `v1.1.0`

Spark SQL example:
```Scala

val biwiseandDF = spark.sql("select RS_BitwiseAND(band1, band2) as andvalue from dataframe")

```

## RS_BitwiseOR

Introduction: Find Bitwise OR between two bands of Geotiff image

Format: `RS_BitwiseOR (Band1: Array[Double], Band2: Array[Double])`

Since: `v1.1.0`

Spark SQL example:
```Scala

val biwiseorDF = spark.sql("select RS_BitwiseOR(band1, band2) as or from dataframe")

```

## RS_SquareRoot

Introduction: Find Square root of band values in a geotiff image 

Format: `RS_SquareRoot (Band: Array[Double])`

Since: `v1.1.0`

Spark SQL example:
```Scala

val rootDF = spark.sql("select RS_SquareRoot(band) as squareroot from dataframe")

```

## RS_LogicalDifference

Introduction: Return value from band 1 if a value in band1 and band2 are different, else return 0

Format: `RS_LogicalDifference (Band1: Array[Double], Band2: Array[Double])`

Since: `v1.1.0`

Spark SQL example:
```Scala

val logicalDifference = spark.sql("select RS_LogicalDifference(band1, band2) as logdifference from dataframe")

```

## RS_LogicalOver

Introduction: Return value from band1 if it's not equal to 0, else return band2 value 

Format: `RS_LogicalOver (Band1: Array[Double], Band2: Array[Double])`

Since: `v1.1.0`

Spark SQL example:
```Scala

val logicalOver = spark.sql("select RS_LogicalOver(band1, band2) as logover from dataframe")

```

## RS_FetchRegion

Introduction: Fetch a subset of region from given Geotiff image based on minimumX, minimumY, maximumX and maximumY index as well original height and width of image

Format: `RS_FetchRegion (Band: Array[Double], coordinates: Array[Int], dimenstions: Array[Int])`

Since: `v1.1.0`

Spark SQL example:
```Scala

val region = spark.sql("select RS_FetchRegion(Band,Array(0, 0, 1, 2),Array(3, 3)) as Region from dataframe")
```

## RS_Normalize

Introduction: Normalize the value in the array to [0, 255]

Since: `v1.1.0`

Spark SQL example
```SQL
SELECT RS_Normalize(band)
```

## RS_AppendNormalizedDifference

Introduction: Appends a Normalized Difference Index to the Geotiff Image Data as a new Band and returns the new data (example: NBR, NDBI) 

Format: `RS_AppendNormalizedDifference (data: Array[Double], indexBand1: Int, indexBand2: Int, nBands: Int)`

The normalized difference index is calculated for bands indexed at `indexBand1` and `indexBand2` in `data` array. `nBands` denotes total number of bands in `data` array. It returns the `data` array after appending the normalized difference index to it.

!!!note
	Index of Geotiff bands starts from 1 (instead of 0). Index of the first band is 1.

Since: `v1.2.1`

Spark SQL example:
```Scala

val dfAppendedNormDiff = spark.sql("select RS_AppendNormalizedDifference(data, index1, index2, nBands) as dataEdited from dataframe")

```

#### Detailed Explanation:

If `band1` is the band indexed at `indexBand1` and `band2` is the band indexed at `indexBand2`, normalized difference index is calculated with the following formula:

`normalized difference index = (band1 - band2)/(band1 + band2 + EPSILON)`

When `(band1 + band2)` is 0, `EPSILON` is 1e-10. Otherwise, `EPSILON` is 0. Difference types of normalized difference indexes are listed below:

##### Normalized Burn Ratio (NBR): [Link Here](https://www.sciencebase.gov/catalog/item/4f4e4b20e4b07f02db6abb36)

```html
 |-- indexBand1: index of the Near Infrared (NIR) band in the image
 |-- indexBand2: index of the Short-wave Infrared (SWIR) band in the image
```

##### Normalized Difference Red Edge Vegetation Index (NDRE): [Link Here](https://agris.fao.org/agris-search/search.do?recordID=US201300795763)

```html
 |-- indexBand1: index of the NIR band in the image
 |-- indexBand2: index of the Red Edge band in the image
```

##### Green Normalized Difference Vegetation Index (GNDVI): [Link Here](https://doi.org/10.2134/agronj2001.933583x)

```html
 |-- indexBand1: index of the NIR band in the image
 |-- indexBand2: index of the Green band in the image
```

##### Normalized Difference Built-up Index (NDBI): [Link Here](https://doi.org/10.1080/01431160304987)

```html
 |-- indexBand1: index of the Short-wave Infrared (SWIR) band in the image
 |-- indexBand2: index of the Near Infrared (NIR) band in the image
```

##### Blue Normalized Difference Vegetation Index (BNDVI): [Link Here](https://doi.org/10.1016/S1672-6308(07)60027-4)

```html
 |-- indexBand1: index of the NIR band in the image
 |-- indexBand2: index of the Blue band in the image
```

##### Normalized Difference Snow Index (NDSI): [Link Here](https://doi.org/10.1109/IGARSS.1994.399618)

```html
 |-- indexBand1: index of the Green band in the image
 |-- indexBand2: index of the Short-wave Infrared (SWIR) band in the image
```

##### Normalized Difference Vegetation Index (NDVI): [Link Here](https://doi.org/10.1016/0034-4257(79)90013-0)

```html
 |-- indexBand1: index of the Red band in the image
 |-- indexBand2: index of the Near Infrared (NIR) band in the image
```

##### Normalized Difference Water Index (NDWI): [Link Here](https://doi.org/10.1080/01431169608948714)

```html
 |-- indexBand1: index of the Green band in the image
 |-- indexBand2: index of the Near Infrared (NIR) band in the image
```

##### Standardized Water-Level Index (SWI): [Link Here](https://doi.org/10.3390/w13121647)

```html
 |-- indexBand1: index of the VRE1 band
 |-- indexBand2: index of the SWIR2 band
```
