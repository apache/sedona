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

# ST_PointM

Introduction: Construct a Point from X, Y and M and an optional srid. If srid is not set, it defaults to 0 (unknown).
Must use ST_AsEWKT function to print the Z and M coordinates.

Format:

`ST_PointM (X: Double, Y: Double, M: Double)`

`ST_PointM (X: Double, Y: Double, M: Double, srid: Integer)`

Return type: `Geometry`

Since: `v1.6.1`

Example:

```sql
SELECT ST_AsEWKT(ST_PointM(1.2345, 2.3456, 3.4567))
```

Output:

```
POINT ZM(1.2345 2.3456 0 3.4567)
```
