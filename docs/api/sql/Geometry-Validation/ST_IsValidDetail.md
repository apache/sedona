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

# ST_IsValidDetail

Introduction: Returns a row, containing a boolean `valid` stating if a geometry is valid, a string `reason` stating why it is invalid and a geometry `location` pointing out where it is invalid.

This function is a combination of [ST_IsValid](ST_IsValid.md) and [ST_IsValidReason](ST_IsValidReason.md).

The flags parameter is a bitfield with the following options:

- 0 (default): Use usual OGC SFS (Simple Features Specification) validity semantics.
- 1: "ESRI flag", Accepts certain self-touching rings as valid, which are considered invalid under OGC standards.

Formats:

```sql
ST_IsValidDetail(geom: Geometry)
```

```sql
ST_IsValidDetail(geom: Geometry, flag: Integer)
```

Return type: `Struct<valid: Boolean, reason: String, location: Geometry>`

Since: `v1.6.1`

SQL Example:

```sql
SELECT ST_IsValidDetail(ST_GeomFromWKT('POLYGON ((30 10, 40 40, 20 40, 30 10, 10 20, 30 10))'))
```

Output:

```
+-----+---------------------------------------------------------+-------------+
|valid|reason                                                   |location     |
+-----+---------------------------------------------------------+-------------+
|false|Ring Self-intersection at or near point (30.0, 10.0, NaN)|POINT (30 10)|
+-----+---------------------------------------------------------+-------------+
```
