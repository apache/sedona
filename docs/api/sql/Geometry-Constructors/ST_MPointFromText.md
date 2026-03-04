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

# ST_MPointFromText

Introduction: Constructs a MultiPoint from the WKT with the given SRID. If SRID is not provided then it defaults to 0. It returns `null` if the WKT is not a `MULTIPOINT`.

Format:

`ST_MPointFromText (Wkt: String)`

`ST_MPointFromText (Wkt: String, srid: Integer)`

Return type: `Geometry`

Since: `v1.6.1`

SQL Example:

```sql
SELECT ST_MPointFromText('MULTIPOINT ((10 10), (20 20), (30 30))')
```

Output:

```
MULTIPOINT ((10 10), (20 20), (30 30))
```
