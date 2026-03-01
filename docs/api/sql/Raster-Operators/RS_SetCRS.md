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

# RS_SetCRS

Introduction: Sets the coordinate reference system (CRS) of a raster using a CRS definition string. Unlike `RS_SetSRID` which only accepts integer EPSG codes, `RS_SetCRS` accepts CRS definitions in multiple formats including EPSG codes, WKT1, WKT2, PROJ strings, and PROJJSON. This function does not reproject/transform the raster data — it only sets the CRS metadata.

Format: `RS_SetCRS (raster: Raster, crsString: String)`

Since: `v1.9.0`

## Supported CRS formats

| Format | Example |
| :--- | :--- |
| EPSG code | `'EPSG:4326'` |
| WKT1 | `'GEOGCS["WGS 84", DATUM["WGS_1984", ...], ...]'` |
| WKT2 | `'GEOGCRS["WGS 84", DATUM["World Geodetic System 1984", ...], ...]'` |
| PROJ string | `'+proj=longlat +datum=WGS84 +no_defs'` |
| PROJJSON | `'{"type": "GeographicCRS", "name": "WGS 84", ...}'` |

## SQL Examples

Setting CRS with an EPSG code:

```sql
SELECT RS_SetCRS(raster, 'EPSG:4326') FROM raster_table
```

Setting CRS with a PROJ string (useful for custom projections):

```sql
SELECT RS_SetCRS(raster, '+proj=lcc +lat_1=25 +lat_2=60 +lat_0=42.5 +lon_0=-100 +x_0=0 +y_0=0 +datum=WGS84 +units=m +no_defs')
FROM raster_table
```

Setting CRS with a WKT1 string:

```sql
SELECT RS_SetCRS(raster, 'GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563]],PRIMEM["Greenwich",0],UNIT["degree",0.0174532925199433]]')
FROM raster_table
```

!!!note
    For standard projections (UTM, Lambert Conformal Conic, Transverse Mercator, etc.), all input formats are fully supported. For exotic CRS definitions not representable as WKT1, there may be minor parameter loss during internal conversion. If your CRS has a known EPSG code, using `'EPSG:xxxx'` provides the most reliable result.
