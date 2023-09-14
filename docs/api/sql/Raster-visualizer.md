Sedona offers some APIs to aid in easy visualization of a raster object.

## Image-based visualization
Sedona offers APIs to visualize a raster in an image form. This API only works for rasters with byte data, and bands <= 4 (Grayscale - RGBA). You can check the data type of an existing raster by using [RS_BandDataType](../Raster-operators/#rs_bandpixeltype) or create your own raster by passing 'B' while using [RS_MakeEmptyRaster](../Raster-loader/#rs_makeemptyraster).

### RS_AsBase64
Introduction: Returns a base64 encoded string of the given raster. This function internally takes the first 4 bands as RGBA, and converts them to the PNG format, finally produces a base64 string. To visualize other bands, please use it together with `RS_Band`. You can take the resulting base64 string in [an online viewer](https://base64-viewer.onrender.com/) to check how the image looks like.

Since: `v1.5.0`

Format: `RS_AsBase64(raster: Raster)`

Spark SQL Example:

```sql
SELECT RS_AsBase64(raster) from rasters
```

Output:

```
iVBORw0KGgoAAAA...
```

### RS_AsImage
Introduction: Returns a HTML that when rendered using an HTML viewer or via a Jupyter Notebook, displays the raster as an image. Optionally, an imageSize parameter can be passed to RS_AsImage in order to increase the size of the rendered image (default: 200).

Since: `1.5.0`

Format: `RS_AsImage(raster: Raster, imageSize: int = 200)`

Spark SQL Example:

```sql
SELECT RS_AsImage(raster, 500) from rasters
SELECT RS_AsImage(raster) from rasters
```

!!!Tip
    RS_AsImage can be paired with SedonaVisualization.display_image(df) wrapper inside a Jupyter notebook to directly print the raster as an image in the output, where the 'df' parameter is the dataframe containing the HTML data provided by RS_AsImage

Output:
```html
"<img src=\"data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAUAAAAECAAAAABjWKqcAAAAIElEQVR42mPgPfGfkYUhhfcBNw+DT1KihS6DqLKztjcATWMFp9rkkJgAAAAASUVORK5CYII=\" width=\"200\" />";
```


## Text-based visualization
### RS_AsMatrix

Introduction: Returns a string, that when printed, outputs the raster band as a pretty printed 2D matrix. All the values of the raster are cast to double for the string. RS_AsMatrix allows specifying the number of digits to be considered after the decimal point.
RS_AsMatrix expects a raster, and optionally a band (default: 1) and postDecimalPrecision (default: 6). The band parameter is 1-indexed.

!!!Note
    If the provided band is not present in the raster, RS_AsMatrix throws an IllegalArgumentException

!!!Note
    If the provided raster has integral values, postDecimalPrecision (if any) is simply ignored and integers are printed in the resultant string

Since: `1.5.0`

Format: `RS_AsMatrix(raster: Raster, band: Int = 1, postDecimalPrecision: Int = 6)`

SQL Example: 

```scala
val inputDf = Seq(Seq(1, 3.333333, 4, 0.0001, 2.2222, 9, 10, 11.11111111, 3, 4, 5, 6)).toDF("band")
inputDf.selectExpr("RS_AsMatrix(RS_AddBandFromArray(RS_MakeEmptyRaster(1, 'd', 4, 3, 0, 0, 1, -1, 0, 0, 0), band, 1, 0))").show()
```

Output:
```sql
| 1.00000   3.33333   4.00000   0.00010|
| 2.22220   9.00000  10.00000  11.11111|
| 3.00000   4.00000   5.00000   6.00000|
```

SQL Example:

```scala
val inputDf = Seq(Seq(1, 3, 4, 0, 2, 9, 10, 11, 3, 4, 5, 6)).toDF("band")
inputDf.selectExpr("RS_AsMatrix(RS_AddBandFromArray(RS_MakeEmptyRaster(1, 'i', 4, 3, 0, 0, 1, -1, 0, 0, 0), band, 1, 0))").show()
```

Output:
```sql
| 1   3   4   0|
| 2   9  10  11|
| 3   4   5   6|
```
