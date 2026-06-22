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

# ST_GeogFromEWKT

Introduction: Construct a Geography from OGC Extended WKT.

Format:

`ST_GeogFromEWKT (EWkt: String)`

Return type: `Geography`

Since: `v1.9.1`

Example:

```sql
SELECT ST_GeogFromEWKT('SRID=4326;LINESTRING (0 0, 3 3, 4 4)')
```

Output:

```
LINESTRING (0 0, 3 3, 4 4)
```

The SRID parsed from the EWKT string (here `4326`) is attached to the resulting geography.
