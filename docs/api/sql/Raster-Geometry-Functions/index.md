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

# Raster Geometry Functions

These functions extract geometry representations from rasters.

| Function | Description | Since |
| :--- | :--- | :--- |
| [RS_ConvexHull](RS_ConvexHull.md) | Return the convex hull geometry of the raster including the NoDataBandValue band pixels. For regular shaped and non-skewed rasters, this gives more or less the same result as RS_Envelope and hence ... | v1.5.0 |
| [RS_Envelope](RS_Envelope.md) | Returns the envelope of the raster as a Geometry. | v1.4.0 |
| [RS_MinConvexHull](RS_MinConvexHull.md) | Returns the min convex hull geometry of the raster **excluding** the NoDataBandValue band pixels, in the given band. If no band is specified, all the bands are considered when creating the min conv... | v1.5.0 |
