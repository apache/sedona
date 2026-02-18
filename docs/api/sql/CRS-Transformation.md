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

# CRS Transformation

Sedona provides coordinate reference system (CRS) transformation through the `ST_Transform` function. Since v1.9.0, Sedona uses the proj4sedona library, a pure Java implementation that supports multiple CRS input formats and grid-based transformations.

## Supported CRS Formats

Sedona supports the following formats for specifying source and target coordinate reference systems:

### Authority Code

The most common way to specify a CRS is using an authority code in the format `AUTHORITY:CODE`. Sedona uses [spatialreference.org](https://spatialreference.org/projjson_index.json) as an open-source CRS database, which supports multiple authorities:

| Authority | Description | Example |
|-----------|-------------|---------|
| EPSG | European Petroleum Survey Group | `EPSG:4326`, `EPSG:3857` |
| ESRI | Esri coordinate systems | `ESRI:102008`, `ESRI:54012` |
| IAU | International Astronomical Union (planetary CRS) | `IAU:30100` |
| SR-ORG | User-contributed definitions | `SR-ORG:6864` |

```sql
-- Transform from WGS84 (EPSG:4326) to Web Mercator (EPSG:3857)
SELECT ST_Transform(
    ST_GeomFromText('POINT(-122.4194 37.7749)'),
    'EPSG:4326',
    'EPSG:3857'
) AS transformed_point
```

Output:

```
POINT (-13627665.271218014 4548257.702387721)
```

```sql
-- Transform using ESRI authority code (North America Albers Equal Area Conic)
SELECT ST_Transform(
    ST_GeomFromText('POINT(-122.4194 37.7749)'),
    'EPSG:4326',
    'ESRI:102008'
) AS transformed_point
```

```sql
-- Transform from WGS84 to UTM Zone 10N (EPSG:32610)
SELECT ST_Transform(
    ST_GeomFromText('POLYGON((-122.5 37.5, -122.5 38.0, -122.0 38.0, -122.0 37.5, -122.5 37.5))'),
    'EPSG:4326',
    'EPSG:32610'
) AS transformed_polygon
```

You can browse available CRS codes at [spatialreference.org](https://spatialreference.org/projjson_index.json) or [EPSG.io](https://epsg.io/).

### WKT1 (OGC Well-Known Text)

WKT1 is the OGC Well-Known Text format for CRS definitions. It starts with `PROJCS[...]` for projected CRS or `GEOGCS[...]` for geographic CRS.

```sql
-- Transform using WKT1 format for target CRS
SELECT ST_Transform(
    ST_GeomFromText('POINT(-122.4194 37.7749)'),
    'EPSG:4326',
    'PROJCS["WGS 84 / Pseudo-Mercator",
        GEOGCS["WGS 84",
            DATUM["WGS_1984",
                SPHEROID["WGS 84",6378137,298.257223563]],
            PRIMEM["Greenwich",0],
            UNIT["degree",0.0174532925199433]],
        PROJECTION["Mercator_1SP"],
        PARAMETER["central_meridian",0],
        PARAMETER["scale_factor",1],
        PARAMETER["false_easting",0],
        PARAMETER["false_northing",0],
        UNIT["metre",1],
        AUTHORITY["EPSG","3857"]]'
) AS transformed_point
```

### WKT2 (ISO 19162:2019)

WKT2 is the modern ISO 19162:2019 standard format. It starts with `PROJCRS[...]` for projected CRS or `GEOGCRS[...]` for geographic CRS.

```sql
-- Transform using WKT2 format for target CRS
SELECT ST_Transform(
    ST_GeomFromText('POINT(-122.4194 37.7749)'),
    'EPSG:4326',
    'PROJCRS["WGS 84 / UTM zone 10N",
        BASEGEOGCRS["WGS 84",
            DATUM["World Geodetic System 1984",
                ELLIPSOID["WGS 84",6378137,298.257223563]]],
        CONVERSION["UTM zone 10N",
            METHOD["Transverse Mercator"],
            PARAMETER["Latitude of natural origin",0],
            PARAMETER["Longitude of natural origin",-123],
            PARAMETER["Scale factor at natural origin",0.9996],
            PARAMETER["False easting",500000],
            PARAMETER["False northing",0]],
        CS[Cartesian,2],
            AXIS["easting",east],
            AXIS["northing",north],
        UNIT["metre",1],
        ID["EPSG",32610]]'
) AS transformed_point
```

### PROJ String

PROJ strings provide a compact way to define CRS using projection parameters. They start with `+proj=`.

```sql
-- Transform using PROJ string for UTM Zone 10N
SELECT ST_Transform(
    ST_GeomFromText('POINT(-122.4194 37.7749)'),
    '+proj=longlat +datum=WGS84 +no_defs',
    '+proj=utm +zone=10 +datum=WGS84 +units=m +no_defs'
) AS transformed_point
```

```sql
-- Transform using PROJ string for Lambert Conformal Conic
SELECT ST_Transform(
    ST_GeomFromText('POINT(-122.4194 37.7749)'),
    'EPSG:4326',
    '+proj=lcc +lat_1=33 +lat_2=45 +lat_0=39 +lon_0=-96 +x_0=0 +y_0=0 +datum=NAD83 +units=m +no_defs'
) AS transformed_point
```

### PROJJSON

PROJJSON is a JSON representation of CRS, useful when working with JSON-based workflows.

```sql
-- Transform using PROJJSON for target CRS
SELECT ST_Transform(
    ST_GeomFromText('POINT(-122.4194 37.7749)'),
    'EPSG:4326',
    '{
        "type": "ProjectedCRS",
        "name": "WGS 84 / UTM zone 10N",
        "base_crs": {
            "name": "WGS 84",
            "datum": {
                "type": "GeodeticReferenceFrame",
                "name": "World Geodetic System 1984",
                "ellipsoid": {
                    "name": "WGS 84",
                    "semi_major_axis": 6378137,
                    "inverse_flattening": 298.257223563
                }
            },
            "coordinate_system": {
                "subtype": "ellipsoidal",
                "axis": [
                    {"name": "Longitude", "abbreviation": "lon", "direction": "east", "unit": "degree"},
                    {"name": "Latitude", "abbreviation": "lat", "direction": "north", "unit": "degree"}
                ]
            }
        },
        "conversion": {
            "name": "UTM zone 10N",
            "method": {"name": "Transverse Mercator"},
            "parameters": [
                {"name": "Latitude of natural origin", "value": 0, "unit": "degree"},
                {"name": "Longitude of natural origin", "value": -123, "unit": "degree"},
                {"name": "Scale factor at natural origin", "value": 0.9996},
                {"name": "False easting", "value": 500000, "unit": "metre"},
                {"name": "False northing", "value": 0, "unit": "metre"}
            ]
        },
        "coordinate_system": {
            "subtype": "Cartesian",
            "axis": [
                {"name": "Easting", "abbreviation": "E", "direction": "east", "unit": "metre"},
                {"name": "Northing", "abbreviation": "N", "direction": "north", "unit": "metre"}
            ]
        },
        "id": {"authority": "EPSG", "code": 32610}
    }'
) AS transformed_point
```

## URL CRS Provider

Since v1.9.0, Sedona supports resolving CRS definitions from a remote HTTP server. This is useful when you need custom or internal CRS definitions that are not included in the built-in database, or when you want to use your own CRS definition service.

When configured, the URL provider is consulted **before** the built-in CRS database. If the URL provider returns a valid CRS definition, it is used directly. If the URL returns a 404 or an error, Sedona falls back to the built-in definitions.

### Hosting CRS definitions

You can host your custom CRS definitions on any HTTP-accessible location. Two common approaches:

- **GitHub repository**: Store CRS definition files in a public GitHub repo and use the raw content URL. This is the easiest way to get started — no server infrastructure required.
- **Public S3 bucket**: Upload CRS definition files to an Amazon S3 bucket with public read access and use the S3 static website URL or CloudFront distribution.

Each file should contain a single CRS definition in the format you specify via `spark.sedona.crs.url.format` (PROJJSON, PROJ string, WKT1, or WKT2).

### Configuration

Set the following Spark configuration properties when creating your Sedona session:

```python
config = (
    SedonaContext.builder()
    .config("spark.sedona.crs.url.base", "https://crs.example.com")
    .config("spark.sedona.crs.url.pathTemplate", "/{authority}/{code}.json")
    .config("spark.sedona.crs.url.format", "projjson")
    .getOrCreate()
)
sedona = SedonaContext.create(config)
```

With the default path template, resolving `EPSG:4326` will fetch:

```
https://crs.example.com/epsg/4326.json
```

Only `spark.sedona.crs.url.base` is required. The other two properties have sensible defaults (`/{authority}/{code}.json` and `projjson`).

### Supported response formats

| Format value | Description | Content example |
|-------------|-------------|----------------|
| `projjson` | PROJJSON (default) | `{"type": "GeographicCRS", ...}` |
| `proj` | PROJ string | `+proj=longlat +datum=WGS84 +no_defs` |
| `wkt1` | OGC WKT1 | `GEOGCS["WGS 84", ...]` |
| `wkt2` | ISO 19162 WKT2 | `GEOGCRS["WGS 84", ...]` |

### Example: GitHub repository

Suppose you have a GitHub repo `myorg/crs-definitions` with the following structure:

```
crs-definitions/
  epsg/
    990001.proj
    990002.proj
```

where `epsg/990001.proj` contains a PROJ string like:

```
+proj=merc +a=6378137 +b=6378137 +lat_ts=0 +lon_0=0 +x_0=0 +y_0=0 +k=1 +units=m +no_defs
```

Point Sedona to the raw GitHub content URL:

```python
config = (
    SedonaContext.builder()
    .config(
        "spark.sedona.crs.url.base",
        "https://raw.githubusercontent.com/myorg/crs-definitions/main",
    )
    .config("spark.sedona.crs.url.pathTemplate", "/epsg/{code}.proj")
    .config("spark.sedona.crs.url.format", "proj")
    .getOrCreate()
)
sedona = SedonaContext.create(config)

# Resolves EPSG:990001 from:
# https://raw.githubusercontent.com/myorg/crs-definitions/main/epsg/990001.proj
sedona.sql("""
    SELECT ST_Transform(
        ST_GeomFromText('POINT(-122.4194 37.7749)'),
        'EPSG:4326',
        'EPSG:990001'
    ) AS transformed_point
""").show()
```

### Example: self-hosted CRS server

```python
config = (
    SedonaContext.builder()
    .config("spark.sedona.crs.url.base", "https://crs.mycompany.com")
    .config("spark.sedona.crs.url.pathTemplate", "/epsg/{code}.proj")
    .config("spark.sedona.crs.url.format", "proj")
    .getOrCreate()
)
sedona = SedonaContext.create(config)

# Now ST_Transform will try https://crs.mycompany.com/epsg/3857.proj
# before falling back to built-in definitions
sedona.sql("""
    SELECT ST_Transform(
        ST_GeomFromText('POINT(-122.4194 37.7749)'),
        'EPSG:4326',
        'EPSG:3857'
    ) AS transformed_point
""").show()
```

### Example: custom authority codes

The URL provider is especially useful for custom or internal authority codes that are not in any public database. With the default path template `/{authority}/{code}.json`, the `{authority}` placeholder is replaced by the authority name from the CRS string (lowercased):

```python
config = (
    SedonaContext.builder()
    .config("spark.sedona.crs.url.base", "https://crs.mycompany.com")
    .config("spark.sedona.crs.url.format", "proj")
    .getOrCreate()
)
sedona = SedonaContext.create(config)

# Resolves MYORG:1001 from:
# https://crs.mycompany.com/myorg/1001.json
sedona.sql("""
    SELECT ST_Transform(
        ST_GeomFromText('POINT(-122.4194 37.7749)'),
        'EPSG:4326',
        'MYORG:1001'
    ) AS transformed_point
""").show()
```

### Example: using geometry SRID with URL provider

If the geometry already has an SRID set (e.g., via `ST_SetSRID`), you can omit the source CRS parameter. The source CRS is derived from the geometry's SRID as an EPSG code:

```python
config = (
    SedonaContext.builder()
    .config("spark.sedona.crs.url.base", "https://crs.mycompany.com")
    .config("spark.sedona.crs.url.format", "proj")
    .getOrCreate()
)
sedona = SedonaContext.create(config)

# The source CRS is taken from the geometry's SRID (4326 → EPSG:4326).
# Only the target CRS string is needed.
sedona.sql("""
    SELECT ST_Transform(
        ST_SetSRID(ST_GeomFromText('POINT(-122.4194 37.7749)'), 4326),
        'EPSG:3857'
    ) AS transformed_point
""").show()
```

### Disabling the URL provider

To avoid enabling the URL provider, omit `spark.sedona.crs.url.base` or leave it as an empty string (the default). Note that once a URL provider has been registered in an executor JVM, it remains active for the lifetime of that JVM.

See also: [Configuration parameters](Parameter.md#crs-transformation) for the full list of URL CRS provider settings.

## Grid File Support

Grid files enable high-accuracy datum transformations, such as NAD27 to NAD83 or OSGB36 to ETRS89. Sedona supports loading grid files from multiple sources.

### Grid File Sources

Grid files can be specified using the `+nadgrids` parameter in PROJ strings:

| Source | Format | Example |
|--------|--------|---------|
| Local file | Absolute path | `+nadgrids=/path/to/grid.gsb` |
| PROJ CDN | `@` prefix | `+nadgrids=@us_noaa_conus.tif` |
| HTTPS URL | Full URL | `+nadgrids=https://cdn.proj.org/us_noaa_conus.tif` |

When using the `@` prefix, grid files are automatically fetched from [PROJ CDN](https://cdn.proj.org/).

### Optional vs Mandatory Grids

- **`@` prefix (optional)**: The transformation continues without the grid if it's unavailable. Use this when the grid improves accuracy but isn't required.
- **No prefix (mandatory)**: An error is thrown if the grid file cannot be found.

### SQL Examples with Grid Files

```sql
-- Transform NAD27 to NAD83 using PROJ CDN grid (optional)
SELECT ST_Transform(
    ST_GeomFromText('POINT(-122.4194 37.7749)'),
    '+proj=longlat +datum=NAD27 +no_defs +nadgrids=@us_noaa_conus.tif',
    'EPSG:4269'
) AS transformed_point
```

```sql
-- Transform using mandatory grid file (error if not found)
SELECT ST_Transform(
    ST_GeomFromText('POINT(-122.4194 37.7749)'),
    '+proj=longlat +datum=NAD27 +no_defs +nadgrids=us_noaa_conus.tif',
    'EPSG:4269'
) AS transformed_point
```

```sql
-- Transform OSGB36 to ETRS89 using UK grid
SELECT ST_Transform(
    ST_GeomFromText('POINT(-0.1276 51.5074)'),
    '+proj=longlat +datum=OSGB36 +nadgrids=@uk_os_OSTN15_NTv2_OSGBtoETRS.gsb +no_defs',
    'EPSG:4258'
) AS transformed_point
```

## Coordinate Order

Sedona expects geometries to be in **longitude/latitude (lon/lat)** order. If your data is in lat/lon order, use `ST_FlipCoordinates` to swap the coordinates before transformation.

```sql
-- If your data is in lat/lon order, flip first
SELECT ST_Transform(
    ST_FlipCoordinates(ST_GeomFromText('POINT(37.7749 -122.4194)')),
    'EPSG:4326',
    'EPSG:3857'
) AS transformed_point
```

Sedona automatically handles coordinate order in the CRS definition, ensuring the source and target CRS use lon/lat order internally.

## Using Geometry SRID

If the geometry already has an SRID set, you can omit the source CRS parameter:

```sql
-- Set SRID on geometry and transform using only target CRS
SELECT ST_Transform(
    ST_SetSRID(ST_GeomFromText('POINT(-122.4194 37.7749)'), 4326),
    'EPSG:3857'
) AS transformed_point
```

## See Also

- [ST_Transform](Function.md#st_transform) - Function reference
- [ST_SetSRID](Function.md#st_setsrid) - Set the SRID of a geometry
- [ST_SRID](Function.md#st_srid) - Get the SRID of a geometry
- [ST_FlipCoordinates](Function.md#st_flipcoordinates) - Swap X and Y coordinates
