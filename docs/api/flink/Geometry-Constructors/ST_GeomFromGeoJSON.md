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

# ST_GeomFromGeoJSON

Introduction: Construct a Geometry from GeoJson

Format: `ST_GeomFromGeoJSON (GeoJson: String)`

Since: `v1.2.0`

Example:

```sql
SELECT ST_GeomFromGeoJSON('{
   "type":"Feature",
   "properties":{
      "STATEFP":"01",
      "COUNTYFP":"077",
      "TRACTCE":"011501",
      "BLKGRPCE":"5",
      "AFFGEOID":"1500000US010770115015",
      "GEOID":"010770115015",
      "NAME":"5",
      "LSAD":"BG",
      "ALAND":6844991,
      "AWATER":32636
   },
   "geometry":{
      "type":"Polygon",
      "coordinates":[
         [
            [-87.621765, 34.873444],
            [-87.617535, 34.873369],
            [-87.62119, 34.85053],
            [-87.62144, 34.865379],
            [-87.621765, 34.873444]
         ]
      ]
   }
}')
```

Output:

```
POLYGON ((-87.621765 34.873444, -87.617535 34.873369, -87.62119 34.85053, -87.62144 34.865379, -87.621765 34.873444))
```

Example:

```sql
SELECT ST_GeomFromGeoJSON('{
   "type":"Polygon",
   "coordinates":[
	  [
	 	 [-87.621765, 34.873444],
 		 [-87.617535, 34.873369],
		 [-87.62119, 34.85053],
		 [-87.62144, 34.865379],
		 [-87.621765, 34.873444]
	  ]
   ]
}')
```

Output:

```
POLYGON ((-87.621765 34.873444, -87.617535 34.873369, -87.62119 34.85053, -87.62144 34.865379, -87.621765 34.873444))
```
