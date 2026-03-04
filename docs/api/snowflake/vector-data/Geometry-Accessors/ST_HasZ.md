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

# ST_HasZ

Introduction: Checks for the presence of Z coordinate values representing measures or linear references. Returns true if the input geometry includes an Z coordinate, false otherwise.

Format: `ST_HasZ(geom: Geometry)`

Return type: `Boolean`

SQL Example

```sql
SELECT ST_HasZ(
        ST_GeomFromWKT('LINESTRING Z (30 10 5, 40 40 10, 20 40 15, 10 20 20)')
)
```

Output:

```
True
```
