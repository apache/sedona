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

# RS_MakeEmptyRaster

Introduction: Returns an empty raster geometry. Every band in the raster is initialized to `0.0`.

Since: `v1.5.0`

Format:

```
RS_MakeEmptyRaster(numBands: Integer, bandDataType: String = 'D', width: Integer, height: Integer, upperleftX: Double, upperleftY: Double, cellSize: Double)
```

* NumBands: The number of bands in the raster. If not specified, the raster will have a single band.
* BandDataType: Optional parameter specifying the data types of all the bands in the created raster.
Accepts one of:
    1. "D" - 64 bits Double
    2. "F" - 32 bits Float
    3. "I" - 32 bits signed Integer
    4. "S" - 16 bits signed Short
    5. "US" - 16 bits unsigned Short
    6. "B" - 8 bits unsigned Byte
* Width: The width of the raster in pixels.
* Height: The height of the raster in pixels.
* UpperleftX: The X coordinate of the upper left corner of the raster, in terms of the CRS units.
* UpperleftY: The Y coordinate of the upper left corner of the raster, in terms of the CRS units.
* Cell Size (pixel size): The size of the cells in the raster, in terms of the CRS units.

It uses the default Cartesian coordinate system.

Format:

```
RS_MakeEmptyRaster(numBands: Integer, bandDataType: String = 'D', width: Integer, height: Integer, upperleftX: Double, upperleftY: Double, scaleX: Double, scaleY: Double, skewX: Double, skewY: Double, srid: Integer)
```

* NumBands: The number of bands in the raster. If not specified, the raster will have a single band.
* BandDataType: Optional parameter specifying the data types of all the bands in the created raster.
Accepts one of:
    1. "D" - 64 bits Double
    2. "F" - 32 bits Float
    3. "I" - 32 bits signed Integer
    4. "S" - 16 bits signed Short
    5. "US" - 16 bits unsigned Short
    6. "B" - 8 bits Byte
* Width: The width of the raster in pixels.
* Height: The height of the raster in pixels.
* UpperleftX: The X coordinate of the upper left corner of the raster, in terms of the CRS units.
* UpperleftY: The Y coordinate of the upper left corner of the raster, in terms of the CRS units.
* ScaleX: The scaling factor of the cells on the X axis
* ScaleY: The scaling factor of the cells on the Y axis
* SkewX: The skew of the raster on the X axis, effectively tilting them in the horizontal direction
* SkewY: The skew of the raster on the Y axis, effectively tilting them in the vertical direction
* SRID: The SRID of the raster. Use 0 if you want to use the default Cartesian coordinate system. Use 4326 if you want to use WGS84.

For more information about ScaleX, ScaleY, SkewX, SkewY, please refer to the [Affine Transformations](../Raster-affine-transformation.md) section.

!!!Note
    If any other value than the accepted values for the bandDataType is provided, RS_MakeEmptyRaster defaults to double as the data type for the raster.

Spark SQL example 1 (with 2 bands):

```sql
SELECT RS_MakeEmptyRaster(2, 10, 10, 0.0, 0.0, 1.0)
```

Output:

```
+--------------------------------------------+
|rs_makeemptyraster(2, 10, 10, 0.0, 0.0, 1.0)|
+--------------------------------------------+
|                        GridCoverage2D["g...|
+--------------------------------------------+
```

Spark SQL example 2 (with 2 bands and dataType):

```sql
SELECT RS_MakeEmptyRaster(2, 'I', 10, 10, 0.0, 0.0, 1.0) - Create a raster with integer datatype
```

Output:

```
+--------------------------------------------+
|rs_makeemptyraster(2, 10, 10, 0.0, 0.0, 1.0)|
+--------------------------------------------+
|                        GridCoverage2D["g...|
+--------------------------------------------+
```

Spark SQL example 3 (with 2 bands, scale, skew, and SRID):

```sql
SELECT RS_MakeEmptyRaster(2, 10, 10, 0.0, 0.0, 1.0, -1.0, 0.0, 0.0, 4326)
```

Output:

```
+------------------------------------------------------------------+
|rs_makeemptyraster(2, 10, 10, 0.0, 0.0, 1.0, -1.0, 0.0, 0.0, 4326)|
+------------------------------------------------------------------+
|                                              GridCoverage2D["g...|
+------------------------------------------------------------------+
```

Spark SQL example 4 (with 2 bands, scale, skew, and SRID):

```sql
SELECT RS_MakeEmptyRaster(2, 'F', 10, 10, 0.0, 0.0, 1.0, -1.0, 0.0, 0.0, 4326) - Create a raster with float datatype
```

Output:

```
+------------------------------------------------------------------+
|rs_makeemptyraster(2, 10, 10, 0.0, 0.0, 1.0, -1.0, 0.0, 0.0, 4326)|
+------------------------------------------------------------------+
|                                              GridCoverage2D["g...|
+------------------------------------------------------------------+
```
