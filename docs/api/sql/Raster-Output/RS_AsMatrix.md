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

# RS_AsMatrix

Introduction: Returns a string, that when printed, outputs the raster band as a pretty printed 2D matrix. All the values of the raster are cast to double for the string. RS_AsMatrix allows specifying the number of digits to be considered after the decimal point.
RS_AsMatrix expects a raster, and optionally a band (default: 1) and postDecimalPrecision (default: 6). The band parameter is 1-indexed.

!!!Note
    If the provided band is not present in the raster, RS_AsMatrix throws an IllegalArgumentException

!!!Note
    If the provided raster has integral values, postDecimalPrecision (if any) is simply ignored and integers are printed in the resultant string

!!!note
    If you are using `show()` to display the output, it will show special characters as escape sequences. To get the expected behavior use the following code:

    === "Scala"

        ```scala
        println(df.selectExpr("RS_AsMatrix(rast)").sample(0.5).collect().mkString("\n"))
        ```

    === "Java"

        ```java
        System.out.println(String.join("\n", df.selectExpr("RS_AsMatrix(rast)").sample(0.5).collect()))
        ```

    === "Python"

        ```python
        print("\n".join(df.selectExpr("RS_AsMatrix(rast)").sample(0.5).collect()))
        ```

    The `sample()` function is only there to reduce the data sent to `collect()`, you may also use `filter()` if that's appropriate.

Format:

```
RS_AsMatrix(raster: Raster, band: Integer = 1, postDecimalPrecision: Integer = 6)
```

Return type: `String`

Since: `1.5.0`

SQL Example

```scala
val inputDf = Seq(Seq(1, 3.333333, 4, 0.0001, 2.2222, 9, 10, 11.11111111, 3, 4, 5, 6)).toDF("band")
print(inputDf.selectExpr("RS_AsMatrix(RS_AddBandFromArray(RS_MakeEmptyRaster(1, 'd', 4, 3, 0, 0, 1, -1, 0, 0, 0), band, 1, 0))").sample(0.5).collect()(0))
```

Output:

```sql
| 1.00000   3.33333   4.00000   0.00010|
| 2.22220   9.00000  10.00000  11.11111|
| 3.00000   4.00000   5.00000   6.00000|
```

SQL Example

```scala
val inputDf = Seq(Seq(1, 3, 4, 0, 2, 9, 10, 11, 3, 4, 5, 6)).toDF("band")
print(inputDf.selectExpr("RS_AsMatrix(RS_AddBandFromArray(RS_MakeEmptyRaster(1, 'i', 4, 3, 0, 0, 1, -1, 0, 0, 0), band, 1, 0))").sample(0.5).collect()(0))
```

Output:

```sql
| 1   3   4   0|
| 2   9  10  11|
| 3   4   5   6|
```
