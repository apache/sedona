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

# ST_Dump

Introduction: This function takes a GeometryCollection/Multi Geometry object and returns a set of geometries containing the individual geometries that make up the input geometry. The function is useful for breaking down a GeometryCollection/Multi Geometry into its constituent geometries.

Format: `ST_Dump(geom: geometry)`

SQL example:

```sql
SELECT sedona.ST_AsText(geom)
FROM table(sedona.ST_Dump(sedona.ST_GeomFromText('MULTIPOINT ((10 40), (40 30), (20 20), (30 10))')));
```

Output:

```
POINT (10 40)
POINT (40 30)
POINT (20 20)
POINT (30 10)
```
