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

# ST_GeomCollFromText

Introduction: Constructs a GeometryCollection from the WKT with the given SRID. If SRID is not provided then it defaults to 0. It returns `null` if the WKT is not a `GEOMETRYCOLLECTION`.

![ST_GeomCollFromText](../../../../image/ST_GeomCollFromText/ST_GeomCollFromText.svg "ST_GeomCollFromText")

Format:

`ST_GeomCollFromText (Wkt: String)`

`ST_GeomCollFromText (Wkt: String, srid: Integer)`

Return type: `Geometry`

SQL Example:

```sql
SELECT ST_GeomCollFromText('GEOMETRYCOLLECTION (POINT (50 50), LINESTRING (20 30, 40 60, 80 90), POLYGON ((30 10, 40 20, 30 20, 30 10), (35 15, 45 15, 40 25, 35 15)))')
```

Output:

```
GEOMETRYCOLLECTION (POINT (50 50), LINESTRING (20 30, 40 60, 80 90), POLYGON ((30 10, 40 20, 30 20, 30 10), (35 15, 45 15, 40 25, 35 15)))
```
