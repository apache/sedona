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

# ST_AsText

Introduction: Return the Well-Known Text string representation of a geometry.
It will support M coordinate if present since v1.5.0.

![ST_AsText](../../../image/ST_AsText/ST_AsText.svg "ST_AsText")

Format: `ST_AsText (A: Geometry)`

Return type: `String`

Since: `v1.0.0`

SQL Example

```sql
SELECT ST_AsText(ST_SetSRID(ST_Point(1.0,1.0), 3021))
```

Output:

```
POINT (1 1)
```

SQL Example

```sql
SELECT ST_AsText(ST_MakePointM(1.0, 1.0, 1.0))
```

Output:

```
POINT M(1 1 1)
```

SQL Example

```sql
SELECT ST_AsText(ST_MakePoint(1.0, 1.0, 1.0, 1.0))
```

Output:

```
POINT ZM(1 1 1 1)
```
