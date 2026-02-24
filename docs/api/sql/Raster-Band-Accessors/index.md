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

# Raster Band Accessors

These functions access band-level properties and statistics of raster objects.

| Function | Description | Since |
| :--- | :--- | :--- |
| [RS_Band](RS_Band.md) | Returns a new raster consisting 1 or more bands of an existing raster. It can build new rasters from existing ones, export only selected bands from a multiband raster, or rearrange the order of ban... | v1.5.0 |
| [RS_BandIsNoData](RS_BandIsNoData.md) | Returns true if the band is filled with only nodata values. Band 1 is assumed if not specified. | v1.5.0 |
| [RS_BandNoDataValue](RS_BandNoDataValue.md) | Returns the no data value of the given band of the given raster. If no band is given, band 1 is assumed. The band parameter is 1-indexed. If there is no data value associated with the given band, R... | v1.5.0 |
| [RS_BandPixelType](RS_BandPixelType.md) | Returns the datatype of each pixel in the given band of the given raster in string format. The band parameter is 1-indexed. If no band is specified, band 1 is assumed. | v1.5.0 |
| [RS_Count](RS_Count.md) | Returns the number of pixels in a given band. If band is not specified then it defaults to `1`. | v1.5.0 |
| [RS_SummaryStats](RS_SummaryStats.md) | Returns summary statistic for a particular band based on the `statType` parameter. The function defaults to band index of `1` when `band` is not specified and excludes noDataValue if `excludeNoData... | v1.6.0 |
| [RS_SummaryStatsAll](RS_SummaryStatsAll.md) | Returns summary stats struct consisting of count, sum, mean, stddev, min, max for a given band in raster. If band is not specified then it defaults to `1`. | v1.5.0 |
| [RS_ZonalStats](RS_ZonalStats.md) | This returns a statistic value specified by `statType` over the region of interest defined by `zone`. It computes the statistic from the pixel values within the ROI geometry and returns the result.... | v1.5.1 |
| [RS_ZonalStatsAll](RS_ZonalStatsAll.md) | Returns a struct of statistic values, where each statistic is computed over a region defined by the `zone` geometry. The struct has the following schema: | v1.5.1 |
