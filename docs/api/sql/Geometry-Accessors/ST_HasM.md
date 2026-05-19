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

# ST_HasM

Introduction: Checks for the presence of M coordinate values representing measures or linear references. Returns true if the input geometry includes an M coordinate, false otherwise.

![ST_HasM](../../../image/ST_HasM/ST_HasM.svg "ST_HasM")

Format: `ST_HasM(geom: Geometry)`

Return type: `Boolean`

Since: `v1.6.1`

SQL Example

```sql
SELECT ST_HasM(
        ST_GeomFromWKT('POLYGON ZM ((30 10 5 1, 40 40 10 2, 20 40 15 3, 10 20 20 4, 30 10 5 1))')
)
```

Output:

```
True
```
