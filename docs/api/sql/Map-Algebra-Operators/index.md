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

# Map Algebra Operators

These functions perform per-pixel mathematical operations on raster band arrays.

| Function | Description | Since |
| :--- | :--- | :--- |
| [RS_Add](RS_Add.md) | Add two spectral bands in a Geotiff image | v1.1.0 |
| [RS_Array](RS_Array.md) | Create an array that is filled by the given value | v1.1.0 |
| [RS_BitwiseAND](RS_BitwiseAND.md) | Find Bitwise AND between two bands of Geotiff image | v1.1.0 |
| [RS_BitwiseOR](RS_BitwiseOR.md) | Find Bitwise OR between two bands of Geotiff image | v1.1.0 |
| [RS_CountValue](RS_CountValue.md) | Returns count of a particular value from a spectral band in a raster image | v1.1.0 |
| [RS_Divide](RS_Divide.md) | Divide band1 with band2 from a geotiff image | v1.1.0 |
| [RS_FetchRegion](RS_FetchRegion.md) | Fetch a subset of region from given Geotiff image based on minimumX, minimumY, maximumX and maximumY index as well original height and width of image | v1.1.0 |
| [RS_GreaterThan](RS_GreaterThan.md) | Mask all the values with 1 which are greater than a particular target value | v1.1.0 |
| [RS_GreaterThanEqual](RS_GreaterThanEqual.md) | Mask all the values with 1 which are greater than equal to a particular target value | v1.1.0 |
| [RS_LessThan](RS_LessThan.md) | Mask all the values with 1 which are less than a particular target value | v1.1.0 |
| [RS_LessThanEqual](RS_LessThanEqual.md) | Mask all the values with 1 which are less than equal to a particular target value | v1.1.0 |
| [RS_LogicalDifference](RS_LogicalDifference.md) | Return value from band 1 if a value in band1 and band2 are different, else return 0 | v1.1.0 |
| [RS_LogicalOver](RS_LogicalOver.md) | Return value from band1 if it's not equal to 0, else return band2 value | v1.1.0 |
| [RS_Mean](RS_Mean.md) | Returns Mean value for a spectral band in a Geotiff image | v1.1.0 |
| [RS_Mode](RS_Mode.md) | Returns Mode from a spectral band in a Geotiff image in form of an array | v1.1.0 |
| [RS_Modulo](RS_Modulo.md) | Find modulo of pixels with respect to a particular value | v1.1.0 |
| [RS_Multiply](RS_Multiply.md) | Multiply two spectral bands in a Geotiff image | v1.1.0 |
| [RS_MultiplyFactor](RS_MultiplyFactor.md) | Multiply a factor to a spectral band in a geotiff image | v1.1.0 |
| [RS_Normalize](RS_Normalize.md) | Normalize the value in the array to [0, 255]. Uniform arrays are set to 0 after normalization. | v1.1.0 |
| [RS_NormalizedDifference](RS_NormalizedDifference.md) | Returns Normalized Difference between two bands(band2 and band1) in a Geotiff image(example: NDVI, NDBI) | v1.1.0 |
| [RS_SquareRoot](RS_SquareRoot.md) | Find Square root of band values in a geotiff image | v1.1.0 |
| [RS_Subtract](RS_Subtract.md) | Subtract two spectral bands in a Geotiff image(band2 - band1) | v1.1.0 |
