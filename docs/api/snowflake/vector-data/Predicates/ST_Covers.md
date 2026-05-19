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

# ST_Covers

Introduction: Return true if A covers B

![ST_Covers returning true](../../../../image/ST_Covers/ST_Covers_true.svg "ST_Covers returning true")
![ST_Covers returning false](../../../../image/ST_Covers/ST_Covers_false.svg "ST_Covers returning false")

Format: `ST_Covers (A:geometry, B:geometry)`

Return type: `Boolean`

SQL example:

```sql
SELECT *
FROM pointdf
WHERE ST_Covers(ST_PolygonFromEnvelope(1.0,100.0,1000.0,1100.0), pointdf.arealandmark)
```
