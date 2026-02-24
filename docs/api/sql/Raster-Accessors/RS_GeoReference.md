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

# RS_GeoReference

Introduction: Returns the georeference metadata of raster as a string in GDAL or ESRI format. Default is GDAL if not specified.

For more information about ScaleX, ScaleY, SkewX, SkewY, please refer to the [Affine Transformations](../Raster-affine-transformation.md) section.

!!!note
    If you are using `show()` to display the output, it will show special characters as escape sequences. To get the expected behavior use the following code:

    === "Scala"

        ```scala
        println(df.selectExpr("RS_GeoReference(rast)").sample(0.5).collect().mkString("\n"))
        ```

    === "Java"

        ```java
        System.out.println(String.join("\n", df.selectExpr("RS_GeoReference(rast)").sample(0.5).collect()))
        ```

    === "Python"

        ```python
        print("\n".join(df.selectExpr("RS_GeoReference(rast)").sample(0.5).collect()))
        ```

    The `sample()` function is only there to reduce the data sent to `collect()`, you may also use `filter()` if that's appropriate.

Format: `RS_GeoReference(raster: Raster, format: String = "GDAL")`

Since: `v1.5.0`

Difference between format representation is as follows:

`GDAL`

```
ScaleX
SkewY
SkewX
ScaleY
UpperLeftX
UpperLeftY
```

`ESRI`

```
ScaleX
SkewY
SkewX
ScaleY
UpperLeftX + ScaleX * 0.5
UpperLeftY + ScaleY * 0.5
```

SQL Example

```sql
SELECT RS_GeoReference(ST_MakeEmptyRaster(1, 100, 100, -53, 51, 2, -2, 4, 5, 4326))
```

Output:

```
2.000000
5.000000
4.000000
-2.000000
-53.000000
51.000000
```

SQL Example

```sql
SELECT RS_GeoReference(ST_MakeEmptyRaster(1, 3, 4, 100.0, 200.0,2.0, -3.0, 0.1, 0.2, 0), "GDAL")
```

Output:

```
2.000000
0.200000
0.100000
-3.000000
100.000000
200.000000
```

SQL Example

```sql
SELECT RS_GeoReference(ST_MakeEmptyRaster(1, 3, 4, 100.0, 200.0,2.0, -3.0, 0.1, 0.2, 0), "ERSI")
```

```
2.000000
0.200000
0.100000
-3.000000
101.000000
198.500000
```
