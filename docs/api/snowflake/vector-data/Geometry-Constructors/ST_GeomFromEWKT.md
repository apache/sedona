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

# ST_GeomFromEWKT

Introduction: Construct a Geometry from OGC Extended WKT

![ST_GeomFromEWKT](../../../../image/ST_GeomFromEWKT/ST_GeomFromEWKT.svg "ST_GeomFromEWKT")

Format:
`ST_GeomFromEWKT (EWkt:string)`

Return type: `Geometry`

SQL example:

```sql
SELECT ST_AsText(ST_GeomFromEWKT('SRID=4269;POINT(40.7128 -74.0060)'))
```

Output:

```
POINT(40.7128 -74.006)
```
