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

# ST_ConvexHull

Introduction: Return the Convex Hull of polygon A

Format: `ST_ConvexHull (A: Geometry)`

Since: `v1.5.0`

Example:

```sql
SELECT ST_ConvexHull(ST_GeomFromText('POLYGON((175 150, 20 40, 50 60, 125 100, 175 150))'))
```

Output:

```
POLYGON ((20 40, 175 150, 125 100, 20 40))
```
