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

# ST_GeomFromWKT

Introduction: Construct a Geometry from WKT. If SRID is not set, it defaults to 0 (unknown).

Format:

`ST_GeomFromWKT (Wkt: String)`

`ST_GeomFromWKT (Wkt: String, srid: Integer)`

Since: `v1.0.0`

The optional srid parameter was added in `v1.3.1`

SQL Example

```sql
SELECT ST_GeomFromWKT('POINT(40.7128 -74.0060)')
```

Output:

```
POINT(40.7128 -74.006)
```
