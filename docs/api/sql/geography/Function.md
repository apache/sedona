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

## ST_Envelope

Introduction: This function returns the bounding box (envelope) of A. It's important to note that the bounding box is calculated using a cylindrical topology, not a spherical one. If the envelope crosses the antimeridian (the 180° longitude line), you can set the split parameter to true. This will return a Geography object containing two separate Polygon objects, split along that line.

Format:

`ST_Envelope (A: Geography, splitAtAntiMeridian: Boolean)`

Since: `v1.8.0`

SQL Example

```sql
SELECT ST_Envelope(ST_GeogFromWKT('MULTIPOLYGON (((177.285 -18.28799, 180 -18.28799, 180 -16.02088, 177.285 -16.02088, 177.285 -18.28799)), ((-180 -18.28799, -179.7933 -18.28799, -179.7933 -16.02088, -180 -16.02088, -180 -18.28799)))'), false);
```

Output:

```
POLYGON ((177.3 -18.3, -179.8 -18.3, -179.8 -16, 177.3 -16, 177.3 -18.3))
```

## ST_AsEWKT

Introduction: Return the Extended Well-Known Text representation of a geography.
EWKT is an extended version of WKT which includes the SRID of the geography.
The format originated in PostGIS but is supported by many GIS tools.

Format: `ST_AsEWKT (A: Geography)`

Since: `v1.8.0`

SQL Example

```sql
SELECT ST_AsEWKT(ST_GeogFromWKT('LINESTRING (1 2, 3 4, 5 6)', 4326))
```

Output:

```
SRID=4326; LINESTRING (1 2, 3 4, 5 6)
```
