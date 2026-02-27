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

# ST_Point

Introduction: Construct a Point from X and Y

Format: `ST_Point (X:decimal, Y:decimal)`

In `v1.4.0` an optional Z parameter was removed to be more consistent with other spatial SQL implementations.
If you are upgrading from an older version of Sedona - please use ST_PointZ or ST_PointZM to create 3D points.

SQL example:

```sql
SELECT ST_Point(double(1.2345), 2.3456)
```

Output:

```
POINT (1.2345 2.3456)
```
