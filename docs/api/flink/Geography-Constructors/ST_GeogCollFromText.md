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

# ST_GeogCollFromText

Introduction: Constructs a GeometryCollection geography from the WKT with the given SRID. If SRID is not provided then it defaults to 0. It returns `null` if the WKT is not a `GEOMETRYCOLLECTION`.

Format:

`ST_GeogCollFromText (Wkt: String)`

`ST_GeogCollFromText (Wkt: String, srid: Integer)`

Return type: `Geography`

Since: `v1.9.1`

Example:

```sql
SELECT ST_GeogCollFromText('GEOMETRYCOLLECTION (POINT (1 2), LINESTRING (0 0, 1 1))')
```

Output:

```
GEOMETRYCOLLECTION (POINT (1 2), LINESTRING (0 0, 1 1))
```
