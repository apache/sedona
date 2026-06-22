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

# ST_Length

Introduction: Return the spherical length of a geography in meters, summed along great-circle edges.

![ST_Length on a sphere: great-circle length](../../../image/ST_Length_geography/ST_Length_geography.svg "ST_Length on a sphere: great-circle length")

Format:

`ST_Length (geog: Geography)`

Return type: `Double`

Since: `v1.9.1`

SQL Example:

```sql
SELECT ST_Length(ST_GeogFromWKT('LINESTRING (0 0, 0 1)', 4326))
```

Output:

```
111195.0662708989
```
