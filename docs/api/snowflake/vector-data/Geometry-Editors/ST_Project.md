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

# ST_Project

Introduction: Calculates a new point location given a starting point, distance, and azimuth. The azimuth indicates the direction, expressed in radians, and is measured in a clockwise manner starting from true north. The system can handle azimuth values that are negative or exceed 2π (360 degrees). The optional `lenient` parameter prevents an error if the input geometry is not a Point. Its default value is `false`.

Format:

```
ST_Project(point: Geometry, distance: Double, azimuth: Double, lenient: Boolean = False)
```

```
ST_Project(point: Geometry, distance: Double, Azimuth: Double)
```

SQL Example:

```sql
SELECT ST_Project(ST_GeomFromText('POINT (10 15)'), 100, radians(90))
```

Output:

```
POINT (110 14.999999999999975)
```

SQL Example:

```sql
SELECT ST_Project(
        ST_GeomFromText('POLYGON ((1 5, 1 1, 3 3, 5 3, 1 5))'),
        25, radians(270), true)
```

Output:

```
POINT EMPTY
```
