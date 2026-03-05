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

# ST_Y

Introduction: Returns Y Coordinate of given Point, null otherwise.

![ST_Y](../../../../image/ST_Y/ST_Y.svg "ST_Y")

Format: `ST_Y(pointA: Point)`

Return type: `Double`

SQL example:

```sql
SELECT ST_Y(ST_POINT(0.0 25.0))
```

Output: `25.0`
