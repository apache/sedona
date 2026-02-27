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

# ST_RelateMatch

Introduction: This function tests the relationship between two [Dimensionally Extended 9-Intersection Model (DE-9IM)](https://en.wikipedia.org/wiki/DE-9IM) matrices representing geometry intersections. It evaluates whether the DE-9IM matrix specified in `matrix1` satisfies the intersection pattern defined by `matrix2`. The `matrix2` parameter can be an exact DE-9IM value or a pattern containing wildcard characters.

!!!Note
    It is important to note that this function is not optimized for use in spatial join operations. Certain DE-9IM relationships can hold true for geometries that do not intersect or are disjoint. As a result, it is recommended to utilize other dedicated spatial functions specifically optimized for spatial join processing.

Format: `ST_RelateMatch(matrix1: String, matrix2: String)`

SQL Example:

```sql
SELECT ST_RelateMatch('101202FFF', 'TTTTTTFFF')
```

Output:

```
true
```
