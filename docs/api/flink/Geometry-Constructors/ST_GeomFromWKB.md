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

# ST_GeomFromWKB

Introduction: Construct a Geometry from WKB string or Binary. This function also supports EWKB format.

Format:

`ST_GeomFromWKB (Wkb: String)`

`ST_GeomFromWKB (Wkb: Binary)`

Since: `v1.2.0`

Example:

```sql
SELECT ST_GeomFromWKB([01 02 00 00 00 02 00 00 00 00 00 00 00 84 D6 00 C0 00 00 00 00 80 B5 D6 BF 00 00 00 60 E1 EF F7 BF 00 00 00 80 07 5D E5 BF])
```

Output:

```
LINESTRING (-2.1047439575195312 -0.354827880859375, -1.49606454372406 -0.6676061153411865)
```

Example:

```sql
SELECT ST_asEWKT(ST_GeomFromWKB('01010000a0e6100000000000000000f03f000000000000f03f000000000000f03f'))
```

Output:

```
SRID=4326;POINT Z(1 1 1)
```

Format:
`ST_GeomFromWKB (Wkb: Bytes)`

Since: `v1.2.1`

Example:

```sql
SELECT ST_GeomFromWKB(polygontable._c0) AS polygonshape
FROM polygontable
```
