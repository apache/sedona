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

# ST_BestSRID

Introduction: Returns the estimated most appropriate Spatial Reference Identifier (SRID) for a given geometry, based on its spatial extent and location. It evaluates the geometry's bounding envelope and selects an SRID that optimally represents the geometry on the Earth's surface. The function prioritizes Universal Transverse Mercator (UTM), Lambert Azimuthal Equal Area (LAEA), or falls back to the Mercator projection. The function takes a WGS84 geometry and must be in ==lon/lat== order.

- For geometries in the Arctic or Antarctic regions, the Lambert Azimuthal Equal Area projection is used.
- For geometries that fit within a single UTM zone and do not cross the International Date Line (IDL), a corresponding UTM SRID is chosen.
- In cases where none of the above conditions are met, the function defaults to the Mercator projection.
- For Geometries that cross the IDL, `ST_BestSRID` defaults the SRID to Mercator. Currently, `ST_BestSRID` does not handle geometries crossing the IDL.

!!!Warning
    `ST_BestSRID` is designed to estimate a suitable SRID from a set of approximately 125 EPSG codes and works best for geometries that fit within the UTM zones. It should not be solely relied upon to determine the most accurate SRID, especially for specialized or high-precision spatial requirements.

Format: `ST_BestSRID(geom: Geometry)`
Return type: `Integer`
