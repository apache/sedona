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

# Pixel Functions

These functions work with individual pixel geometry representations.

| Function | Description | Since |
| :--- | :--- | :--- |
| [RS_PixelAsCentroid](RS_PixelAsCentroid.md) | Returns the centroid (point geometry) of the specified pixel's area. The pixel coordinates specified are 1-indexed. If `colX` and `rowY` are out of bounds for the raster, they are interpolated assu... | v1.5.0 |
| [RS_PixelAsCentroids](RS_PixelAsCentroids.md) | Returns a list of the centroid point geometry, the pixel value and its raster X and Y coordinates for each pixel in the raster at the specified band. Each centroid represents the geometric center o... | v1.5.1 |
| [RS_PixelAsPoint](RS_PixelAsPoint.md) | Returns a point geometry of the specified pixel's upper-left corner. The pixel coordinates specified are 1-indexed. | v1.5.0 |
| [RS_PixelAsPoints](RS_PixelAsPoints.md) | Returns a list of the pixel's upper-left corner point geometry, the pixel value and its raster X and Y coordinates for each pixel in the raster at the specified band. | v1.5.1 |
| [RS_PixelAsPolygon](RS_PixelAsPolygon.md) | Returns a polygon geometry that bounds the specified pixel. The pixel coordinates specified are 1-indexed. If `colX` and `rowY` are out of bounds for the raster, they are interpolated assuming the ... | v1.5.0 |
| [RS_PixelAsPolygons](RS_PixelAsPolygons.md) | Returns a list of the polygon geometry, the pixel value and its raster X and Y coordinates for each pixel in the raster at the specified band. | v1.5.1 |
