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

# ST_GeometryFromText

Introduction: Construct a Geometry from WKT. If SRID is not set, it defaults to 0 (unknown). Alias of [ST_GeomFromWKT](ST_GeomFromWKT.md)

Format:

`ST_GeometryFromText (Wkt: String)`

`ST_GeometryFromText (Wkt: String, srid: Integer)`

Return type: `Geometry`

Since: `v1.6.1`

SQL Example

```sql
SELECT ST_GeometryFromText('POINT(40.7128 -74.0060)')
```

Output:

```
POINT(40.7128 -74.006)
```
