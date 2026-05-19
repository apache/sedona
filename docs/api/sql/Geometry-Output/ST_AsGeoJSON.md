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

# ST_AsGeoJSON

!!!note
	This method is not recommended. Please use [Sedona GeoJSON data source](../../../tutorial/sql.md#save-geojson) to write GeoJSON files.

Introduction: Return the [GeoJSON](https://geojson.org/) string representation of a geometry

The type parameter (Since: `v1.6.1`) takes the following options -

- "Simple" (default): Returns a simple GeoJSON geometry.
- "Feature": Wraps the geometry in a GeoJSON Feature.
- "FeatureCollection": Wraps the Feature in a GeoJSON FeatureCollection.

Format:

`ST_AsGeoJSON (A: Geometry)`

`ST_AsGeoJSON (A: Geometry, type: String)`

Return type: `String`

Since: `v1.0.0`

SQL Example (Simple GeoJSON):

```sql
SELECT ST_AsGeoJSON(ST_GeomFromWKT('POLYGON((1 1, 8 1, 8 8, 1 8, 1 1))'))
```

Output:

```json
{
  "type":"Polygon",
  "coordinates":[
    [[1.0,1.0],
      [8.0,1.0],
      [8.0,8.0],
      [1.0,8.0],
      [1.0,1.0]]
  ]
}
```

SQL Example (Feature GeoJSON):

Output:

```json
{
  "type":"Feature",
  "geometry": {
      "type":"Polygon",
      "coordinates":[
        [[1.0,1.0],
          [8.0,1.0],
          [8.0,8.0],
          [1.0,8.0],
          [1.0,1.0]]
      ]
  }
}
```

SQL Example (FeatureCollection GeoJSON):

Output:

```json
{
  "type":"FeatureCollection",
  "features": [{
      "type":"Feature",
      "geometry": {
          "type":"Polygon",
          "coordinates":[
            [[1.0,1.0],
              [8.0,1.0],
              [8.0,8.0],
              [1.0,8.0],
              [1.0,1.0]]
          ]
      }
    }
  ]
}
```
