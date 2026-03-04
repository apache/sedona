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

# ST_Multi

Introduction: Returns a MultiGeometry object based on the geometry input.
ST_Multi is basically an alias for ST_Collect with one geometry.

![ST_Multi](../../../image/ST_Multi/ST_Multi.svg "ST_Multi")
Format: `ST_Multi(geom: Geometry)`

Return type: `Geometry`

Since: `v1.2.0`

SQL Example

```sql
SELECT ST_Multi(ST_GeomFromText('POINT(1 1)'))
```

Output:

```
MULTIPOINT (1 1)
```
