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

# ST_LineFromWKB

Introduction: Construct a LineString geometry from WKB string or Binary and an optional SRID. This function also supports EWKB format.

!!!note
	Returns null if geometry is not of type LineString.

Format:

`ST_LineFromWKB (Wkb: String)`

`ST_LineFromWKB (Wkb: Binary)`

`ST_LineFromWKB (Wkb: String, srid: Integer)`

`ST_LineFromWKB (Wkb: Binary, srid: Integer)`

Since: `v1.6.1`

Example:

```sql
SELECT ST_LineFromWKB([01 02 00 00 00 02 00 00 00 00 00 00 00 84 D6 00 C0 00 00 00 00 80 B5 D6 BF 00 00 00 60 E1 EF F7 BF 00 00 00 80 07 5D E5 BF])
```

Output:

```
LINESTRING (-2.1047439575195312 -0.354827880859375, -1.49606454372406 -0.6676061153411865)
```
