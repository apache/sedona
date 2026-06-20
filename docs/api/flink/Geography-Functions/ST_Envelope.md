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

# ST_Envelope

Introduction: Return the bounding box (envelope) of a geography. When `splitAtAntiMeridian` is true, an envelope crossing the antimeridian is split into a MultiPolygon.

Format:

`ST_Envelope (geog: Geography, splitAtAntiMeridian: Boolean)`

Return type: `Geography`

Since: `v1.9.1`

SQL Example:

```sql
SELECT ST_Envelope(ST_GeogFromWKT('LINESTRING (1 1, 2 3)', 4326), false)
```

Output:

```
POLYGON ((1 1, 2 1, 2 3, 1 3, 1 1))
```
