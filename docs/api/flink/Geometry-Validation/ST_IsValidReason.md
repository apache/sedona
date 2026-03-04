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

# ST_IsValidReason

Introduction: Returns text stating if the geometry is valid. If not, it provides a reason why it is invalid. The function can be invoked with just the geometry or with an additional flag. The flag alters the validity checking behavior. The flags parameter is a bitfield with the following options:

- 0 (default): Use usual OGC SFS (Simple Features Specification) validity semantics.
- 1: "ESRI flag", Accepts certain self-touching rings as valid, which are considered invalid under OGC standards.

Formats:

```
ST_IsValidReason (A: Geometry)
```

```
ST_IsValidReason (A: Geometry, flag: Integer)
```

Return type: `String`

Since: `v1.5.1`

SQL Example for valid geometry:

```sql
SELECT ST_IsValidReason(ST_GeomFromWKT('POLYGON ((100 100, 100 300, 300 300, 300 100, 100 100))')) as validity_info
```

Output:

```
Valid Geometry
```

SQL Example for invalid geometries:

```sql
SELECT gid, ST_IsValidReason(geom) as validity_info
FROM Geometry_table
WHERE ST_IsValid(geom) = false
ORDER BY gid
```

Output:

```
gid  |                  validity_info
-----+----------------------------------------------------
5330 | Self-intersection at or near point (32.0, 5.0, NaN)
5340 | Self-intersection at or near point (42.0, 5.0, NaN)
5350 | Self-intersection at or near point (52.0, 5.0, NaN)

```
