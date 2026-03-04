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

# ST_GeomFromKML

Introduction: Construct a Geometry from KML.

![ST_GeomFromKML](../../../image/ST_GeomFromKML/ST_GeomFromKML.svg "ST_GeomFromKML")

Format:
`ST_GeomFromKML (kml: String)`

Return type: `Geometry`

Since: `v1.3.0`

SQL example:

```sql
SELECT ST_GeomFromKML('
	<LineString>
		<coordinates>
			-71.1663,42.2614
			-71.1667,42.2616
		</coordinates>
	</LineString>
')
```

Output:

```
LINESTRING (-71.1663 42.2614, -71.1667 42.2616)
```
