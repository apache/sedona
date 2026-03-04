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

# ST_CoordDim

Introduction: Returns the coordinate dimensions of the geometry. It is an alias of `ST_NDims`.

Format: `ST_CoordDim(geom: Geometry)`

Return type: `Integer`

Since: `v1.5.0`

Spark SQL Example with x, y, z coordinate:

```sql
SELECT ST_CoordDim(ST_GeomFromText('POINT(1 1 2'))
```

Output:

```
3
```

Spark SQL Example with x, y coordinate:

```sql
SELECT ST_CoordDim(ST_GeomFromWKT('POINT(3 7)'))
```

Output:

```
2
```
