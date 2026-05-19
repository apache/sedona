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

# Data shipped with the example notebooks

This directory holds small, public-domain or near-public-domain reference datasets that the bundled Jupyter and Zeppelin notebooks read from. The notebooks reach these files via paths like `data/<name>` and the docker image copies them to `/opt/workspace/examples/data/`.

The list below covers data referenced by notebooks at the **root of `docs/usecases/`** (the ones that ship in the docker image). Older files that survive only for `legacy/` notebooks are included for completeness; if you find an entry inaccurate please open a PR — provenance was reconstructed from public sources at audit time.

## Datasets

### `ne_50m_admin_0_countries_lakes/`

- **What:** World country boundaries with internal-lake clipping at 1:50 m scale.
- **Source:** [Natural Earth](https://www.naturalearthdata.com/downloads/50m-cultural-vectors/50m-admin-0-countries/) — Admin 0 – Countries (download → `ne_50m_admin_0_countries_lakes.shp`).
- **License:** Public domain. Per the Natural Earth [terms of use](https://www.naturalearthdata.com/about/terms-of-use/): *"All versions of Natural Earth raster + vector map data found on this website are in the public domain. You may use the maps in any manner."*
- **CRS:** EPSG:4326 (WGS 84, lon/lat).
- **Used by:** `00-quickstart.ipynb`, several `legacy/` notebooks.

### `ne_50m_airports/`

- **What:** World airport point dataset at 1:50 m scale, 891 features.
- **Source:** [Natural Earth](https://www.naturalearthdata.com/downloads/10m-cultural-vectors/airports/) — equivalent 50 m export.
- **License:** Public domain (same Natural Earth terms as above).
- **CRS:** EPSG:4326 (WGS 84, lon/lat).
- **Used by:** `00-quickstart.ipynb`, several `legacy/` notebooks.

### `nyc_taxi_zones/`

- **What:** New York City TLC taxi-zone polygons. 263 zones with `LocationID`, `zone`, `borough` attributes; the `LocationID` column joins to `PULocationID` / `DOLocationID` columns in the TLC trip-record parquet files.
- **Source:** NYC TLC public reference data, distributed by the TLC at <https://d37ci6vzurychx.cloudfront.net/misc/taxi_zones.zip> (linked from the [TLC trip record data page](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)).
- **License:** NYC government open data — published by the NYC Taxi & Limousine Commission for free public use. The TLC releases this dataset alongside the trip-record archive without a separate license file; the [NYC Open Data Terms of Use](https://opendata.cityofnewyork.us/overview/) and the city's [general terms of use](https://www.nyc.gov/home/terms-of-use.page) apply: free use including commercial, no warranty, attribution to NYC TLC requested when redistributed.
- **Attribution:** NYC Taxi & Limousine Commission.
- **CRS:** EPSG:2263 (NAD83 / New York Long Island State Plane, US-feet). The notebook reprojects to EPSG:4326 with `ST_Transform`.
- **Used by:** `01-mobility-pulse.ipynb`.

### `gis_osm_pois_free_1.*`

- **What:** OpenStreetMap point-of-interest extract for a subregion of western Poland (bbox 14.5–16.4 °E, 51.3–53.1 °N), shapefile bundle.
- **Source:** Geofabrik OSM extracts (<https://download.geofabrik.de/>), `gis_osm_pois_free_1` layer; specific country/region not recorded at audit time.
- **License:** [Open Database License (ODbL) 1.0](https://opendatacommons.org/licenses/odbl/1-0/), inherited from OpenStreetMap. Attribution: *"© OpenStreetMap contributors"*. Redistributions of derived works must remain ODbL.
- **CRS:** EPSG:4326 (WGS 84, lon/lat).
- **Used by:** `legacy/ApacheSedonaSQL.ipynb`. Not referenced by any currently shipped (root-level) notebook.

### `raster/`

- **What:** Small GeoTIFF samples used by the legacy raster notebook.
- **Source:** Provided with the original Sedona notebooks in PR [#1336](https://github.com/apache/sedona/pull/1336). Not redistributed from a single upstream source; treated as test fixtures.
- **License:** Inherits Apache 2.0 from the project source tree.
- **Used by:** `legacy/ApacheSedonaRaster.ipynb`.

### `polygon/`, `testpoint.csv`, `testPolygon.json`, `county_small.tsv`, `county_small_wkb.tsv`, `arealm-small.csv`, `primaryroads-linestring.csv`, `primaryroads-polygon.csv`, `zcta510-small.csv`

- **What:** Small synthetic or US-Census-derived test fixtures used by the legacy SQL/RDD notebooks.
- **Source:** Provided with the original Sedona notebooks in PR [#1336](https://github.com/apache/sedona/pull/1336). Census-derived files (ZCTA, county, primary roads) descend from US Census TIGER/Line products, which are in the public domain.
- **License:** Treated as test fixtures shipped under the project's Apache 2.0; underlying TIGER/Line data is public domain.
- **Used by:** various `legacy/` notebooks. None of these are referenced by currently shipped (root-level) notebooks.

## Adding new data

When adding a new dataset to this directory, add an entry above with:

- **What** — one-line description of the data
- **Source** — exact URL or path the file was downloaded from
- **License** — the upstream license, with a link
- **Attribution** — required attribution text, if any
- **CRS** — coordinate reference system (helpful for shipped vector files)
- **Used by** — which notebook(s) reference it

Prefer public-domain, CC0, or other Apache-2.0-compatible licenses. ODbL or share-alike sources require explicit `Attribution` and downstream-license notes. If the dataset is large or its license is unclear, fetch it at runtime from the upstream URL instead of redistributing.
