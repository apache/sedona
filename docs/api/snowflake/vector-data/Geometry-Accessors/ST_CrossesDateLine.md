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

# ST_CrossesDateLine

Introduction: This function determines if a given geometry crosses the International Date Line. It operates by checking if the difference in longitude between any pair of consecutive points in the geometry exceeds 180 degrees. If such a difference is found, it is assumed that the geometry crosses the Date Line. It returns true if the geometry crosses the Date Line, and false otherwise.

!!!note
    The function assumes that the provided geometry is in lon/lat coordinate reference system where longitude values range from -180 to 180 degrees.

!!!note
    For multi-geometries (e.g., MultiPolygon, MultiLineString), this function will return true if any one of the geometries within the multi-geometry crosses the International Date Line.

Format: `ST_CrossesDateLine(geometry: Geometry)`

Return type: `Boolean`

!!!Warning
    For geometries that span more than 180 degrees in longitude without actually crossing the Date Line, this function may still return true, indicating a crossing.
