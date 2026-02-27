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

# ST_MakeEnvelope

Introduction: Construct a Polygon from MinX, MinY, MaxX, MaxY, and an optional SRID.

Format:

```
ST_MakeEnvelope(MinX: Double, MinY: Double, MaxX: Double, MaxY: Double)
```

```
ST_MakeEnvelope(MinX: Double, MinY: Double, MaxX: Double, MaxY: Double, srid: Integer)
```

Since: `v1.7.0`

SQL Example

```sql
SELECT ST_MakeEnvelope(1.234, 2.234, 3.345, 3.345, 4236)
```

Output:

```
POLYGON ((1.234 2.234, 1.234 3.345, 3.345 3.345, 3.345 2.234, 1.234 2.234))
```
