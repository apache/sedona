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

# ST_MakePoint

Introduction: Creates a 2D, 3D Z or 4D ZM Point geometry. Use [ST_MakePointM](ST_MakePointM.md) to make points with XYM coordinates. Z and M values are optional.

![ST_MakePoint](../../../image/ST_MakePoint/ST_MakePoint.svg "ST_MakePoint")

Format: `ST_MakePoint (X: Double, Y: Double, Z: Double, M: Double)`

Return type: `Geometry`

Since: `v1.5.0`

Example:

```sql
SELECT ST_AsText(ST_MakePoint(1.2345, 2.3456));
```

Output:

```
POINT (1.2345 2.3456)
```

Example:

```sql
SELECT ST_AsText(ST_MakePoint(1.2345, 2.3456, 3.4567));
```

Output:

```
POINT Z (1.2345 2.3456 3.4567)
```

Example:

```sql
SELECT ST_AsText(ST_MakePoint(1.2345, 2.3456, 3.4567, 4));
```

Output:

```
POINT ZM (1.2345 2.3456 3.4567 4)
```
