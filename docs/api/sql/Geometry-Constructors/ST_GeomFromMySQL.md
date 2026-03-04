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

# ST_GeomFromMySQL

Introduction: Construct a Geometry from MySQL Geometry binary.

Format: `ST_GeomFromMySQL (binary: Binary)`

Return type: `Geometry`

Since: `v1.8.0`

SQL Example

```sql
SELECT
    ST_GeomFromMySQL(geomWKB) AS geom,
    ST_SRID(ST_GeomFromMySQL(geomWKB)) AS srid
FROM mysql_table
```

Output:

```
+-------------+----+
|         geom|srid|
+-------------+----+
|POINT (20 10)|4326|
|POINT (40 30)|4326|
|POINT (60 50)|4326|
+-------------+----+
```
