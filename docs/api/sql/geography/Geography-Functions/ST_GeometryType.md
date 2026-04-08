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

# ST_GeometryType

Introduction: Returns the type of the geography object as a string (e.g., "ST_Point", "ST_Polygon", "ST_LineString").

Format:

`ST_GeometryType (A: Geography)`

Return type: `String`

Since: `v1.9.0`

SQL Example

```sql
SELECT ST_GeometryType(ST_GeogFromWKT('POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))'));
```

Output:

```
ST_Polygon
```
