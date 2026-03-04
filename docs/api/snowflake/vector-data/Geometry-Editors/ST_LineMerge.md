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

# ST_LineMerge

Introduction: Returns a LineString or MultiLineString formed by sewing together the constituent line work of a MULTILINESTRING.

!!!note
    Only works for MULTILINESTRING. Using other geometry will return a GEOMETRYCOLLECTION EMPTY. If no merging can be performed, the original MULTILINESTRING is returned.

![ST_LineMerge](../../../../image/ST_LineMerge/ST_LineMerge.svg "ST_LineMerge")
Format: `ST_LineMerge (A:geometry)`

Return type: `Geometry`

```sql
SELECT ST_LineMerge(geometry)
FROM df
```
