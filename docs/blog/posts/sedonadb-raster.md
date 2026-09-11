---
date:
  created: 2026-09-11
links:
  - SedonaDB quickstart: https://sedona.apache.org/sedonadb/latest/quickstart-python/
  - RS_Polygonize in SedonaDB: https://sedona.apache.org/sedonadb/latest/reference/sql/rs_polygonize/
  - Planet Crisis Response data: https://source.coop/planet/disasterdata
authors:
  - jia
title: "SELECT * FROM Satellite, in a Rust Database"
---

# SELECT * FROM Satellite, in a Rust Database

Satellite scenes are tables now, in a Rust database engine. [SedonaDB](https://sedona.apache.org/sedonadb/latest/) 0.4.1 ships a raster layer: rasters stream from cloud storage without a download, they clip and reproject in SQL, zonal statistics and polygonization are functions, and pixels move to NumPy and back without a copy. One machine, one `pip install`. The tutorial below runs every piece on one event, the Gironde and Landes wildfire of July 2026, which burned through the pine forest west of Bordeaux and prompted 250,000 evacuations. Eleven PlanetScope scenes go in; burn perimeters, hectares per commune and a GeoTIFF come out.

![Left: the title as a SQL query and three numbers. Right: PlanetScope true-color image of the Gironde coast on 28 July 2026 with the burn scar outlined in red polygons](sedonadb-raster-cover.png)

<!-- more -->

## The catalog is a table

Planet publishes its crisis imagery as public Cloud Optimized GeoTIFFs with a [STAC-GeoParquet](https://source.coop/planet/disasterdata) index per event. SedonaDB reads the index over HTTPS, the footprints are a geometry column, and picking scenes is `ST_Intersects` against the study area:

??? example "Scene selection"

    ```python
    import sedonadb

    sd = sedonadb.connect()
    EV = "https://data.source.coop/planet/disasterdata/gironde-wildfire-2026"
    AOI = "POLYGON((-1.30 44.70, -0.95 44.70, -0.95 45.02, -1.30 45.02, -1.30 44.70))"

    sd.read_parquet(f"{EV}/pre-event/items.parquet").to_view("pre_event")
    sd.read_parquet(f"{EV}/post-event/items.parquet").to_view("post_event")
    scenes = sd.sql(f"""
        SELECT 'pre' AS phase, id, "eo:cloud_cover" AS cloud, assets.analytic_sr.href AS href
        FROM pre_event
        WHERE constellation = 'planetscope' AND ST_Intersects(geometry, ST_GeomFromWKT('{AOI}', 4326))
        UNION ALL
        SELECT 'post', id, "eo:cloud_cover", assets.analytic_sr.href
        FROM post_event
        WHERE constellation = 'planetscope' AND ST_Intersects(geometry, ST_GeomFromWKT('{AOI}', 4326))
        ORDER BY phase DESC, cloud
    """).to_pandas()
    ```

```
   phase                       id  cloud
0    pre  20260708_105558_91_2544      0
1    pre  20260708_105601_27_2544      0
...
8   post  20260728_105820_44_2562      0
9   post  20260728_105825_11_2562      0
10  post  20260728_105822_78_2562      5
```

Eight scenes from 8 July, before the fire started on the 23rd, and three from 28 July, while it was still burning. Two seconds, and no pixel has been read.

## Rasters stay where they are

`RS_FromPath` opens a file's header and returns an out-of-database raster: georeferencing in the value, bands pointing at the source, pixels fetched only when a function asks. A scene is half a gigabyte and opens in about a second:

```
{'w': 12715, 'h': 9085, 'nb': 4, 'srid': 32630, 'sx': 3.0, 't': 'UNSIGNED_16BITS', 'nodata': 0.0}
```

Four bands at 3 m in UTM zone 30, each scene on its own origin. The analysis needs one grid, so the grid is a raster too: a NumPy array of zeros at 12 m over the study area, handed to SQL as a query parameter. `RS_Clip` reads only the window inside the study area and `RS_ReprojectMatch` averages the 3 m pixels onto the 12 m cells. The scenes of a date are rows of one `VALUES` table, so one statement fans the reads out across cores:

??? example "Clip and align every scene of a date in one statement"

    ```python
    import numpy as np
    from sedonadb.raster import Raster

    W, H = 2374, 3017  # 12 m cells over the 28 x 36 km study area, UTM 30N
    GT = [633936.0, 12, 0, 4987224.0, 0, -12]
    ref = Raster.from_numpy(np.zeros((H, W), np.uint8), crs="EPSG:32630", transform=GT)


    def aligned(phase, band, algorithm="Average"):
        rows = scenes[scenes.phase == phase]
        values = ", ".join(f"('{r.id}', '{r.href}')" for r in rows.itertuples())
        tbl = sd.sql(
            f"""
            SELECT id, RS_ReprojectMatch(
                           RS_Clip(RS_FromPath(href), {band}, ST_GeomFromWKT('{AOI}', 4326)),
                           $1, '{algorithm}') AS r
            FROM (VALUES {values}) AS t(id, href)
            """,
            params=(ref,),
        ).to_arrow_table()
        return {
            tbl["id"][i].as_py(): Raster(tbl["r"], i).to_numpy()[0]
            for i in range(tbl.num_rows)
        }
    ```

Red, near-infrared and the usable-data mask each take one statement per date, six in all, and `to_numpy()` hands every result to NumPy as a view. The six statements take eleven and a half minutes together, nearly all of it range requests over HTTPS. A few lines of NumPy stack the scenes into one mosaic per date, first clear pixel wins; the eleven scenes cover 87 percent of the grid, the rest is ocean and the gaps between strips.

## NumPy in the middle

The burn index is the drop in NDVI between the two dates. Otsu's method picks the cutoff from the data, splitting the histogram of that drop over pixels that were vegetated before the fire, and lands at 0.225. The resulting 0/1 array becomes an in-database raster with `Raster.from_numpy`, and the rest of the post is SQL against it.

??? example "NDVI drop to burn mask"

    ```python
    def ndvi(red, nir):
        return np.where(red + nir > 0, (nir - red) / np.maximum(red + nir, 1), np.nan)


    drop = ndvi(pre_red, pre_nir) - ndvi(post_red, post_nir)
    burn = (both_dates & (ndvi(pre_red, pre_nir) > 0.35) & (drop > 0.225)).astype(np.uint8)
    mask = Raster.from_numpy(burn, crs="EPSG:32630", transform=GT)
    ```

## RS_Polygonize: the scar becomes geometry

One function turns connected runs of equal-valued pixels into polygons, and `unnest` turns the list into rows:

```python
sd.sql(
    """
    SELECT p.geom AS geom, ST_Area(p.geom) AS m2
    FROM (SELECT unnest(RS_Polygonize($1, 1)) AS p) WHERE p.value = 1
""",
    params=(mask,),
).to_memtable().to_view("patches")
```

```
        ha                                             c
0  21090.0  POINT(-1.0499563746105551 44.85692261508244)
1    855.0  POINT(-1.1992262050284765 44.79970523209393)
2    303.0  POINT(-0.9643428713835945 44.76723745400395)
```

Polygonizing the 7-million-cell mask takes 2.5 seconds and yields 3,769 patches. Dropping everything under a hectare leaves **173 polygons covering 24,091 ha**, and one of them is the fire: a single 21,090 ha geometry between Le Porge and Lanton.

![PlanetScope true-color image of the coast between Lacanau and the Arcachon Basin on 28 July 2026, the burn scar dark purple, outlined by 173 red polygons; smoke plumes still rise from the western edge](sedonadb-raster-burn.png)

## Zonal statistics on the mask

The same raster parameter works in `RS_ZonalStats`. The sum of a 0/1 mask over a polygon is a pixel count, so hectares per commune, read earlier from Overture's divisions theme, are one multiplication away:

```python
per_commune = sd.sql(
    """
    SELECT name, RS_ZonalStats($1, geometry, 'sum') * 0.0144 AS burned_ha
    FROM communes ORDER BY burned_ha DESC
""",
    params=(mask,),
).to_pandas()
```

![Horizontal bar chart of burned hectares by commune: Le Porge 6,202, Saumos 4,045, Lanton 3,499, Arès 3,498, Lacanau 2,025, Le Temple 1,667, then three communes under 250](sedonadb-raster-communes.svg)

Twenty-seven communes in 12.5 seconds.

## Three rasters, one grid

Any raster aligns to the grid the same way. ESA WorldCover, the 10 m land-cover map from [last week's post](https://sedona.apache.org/latest/blog/2026/09/04/group-by-but-for-pixels/), goes through the same `RS_Clip` and `RS_ReprojectMatch` pair with `NearestNeighbor` in place of `Average`, and a NumPy crosstab says what burned:

```
   land_cover  class  burned_ha  share_pct
   tree cover     10    18262.5       75.0
    grassland     30     5501.0       22.6
     cropland     40      548.7        2.3
     built-up     50       23.7        0.1
```

Three quarters of the scar is the Landes pine forest. The 549 ha of cropland is a caveat worth reading off the map: a few harvested center-pivot fields at the southeast edge lost as much NDVI as a burned stand, and a vegetation-drop index cannot tell the two apart. The land-cover column is how that gets filtered when it matters.

## Write it back

`RS_AsGeoTiff` encodes the mask as a tiled, compressed GeoTIFF of 137 KB, and the polygons leave as GeoParquet through `to_parquet`.

??? example "Export"

    ```python
    tif = sd.sql(
        "SELECT RS_AsGeoTiff($1, 'DEFLATE', 0.0, 256) AS tif", params=(mask,)
    ).to_arrow_table()
    open("burn_mask.tif", "wb").write(tif["tif"][0].as_py())
    sd.sql(
        "SELECT ST_Transform(geom, 'EPSG:4326') AS geom, m2 FROM patches WHERE m2 >= 10000"
    ).to_parquet("burn_patches.parquet")
    ```

## What the numbers are and are not

The whole pipeline ran in twelve minutes in one process, and all but thirty seconds of that was streaming pixels. The 24,091 ha is the scar visible on the morning of 28 July at 12 m, with the pixels under that morning's smoke plumes masked out by Planet's usable-data layer; the fire kept burning after those scenes, and the estimates published for the whole complex run from 34,000 to 45,000 ha. Rerunning against a later scene is a change of one line in the catalog query.

## The point

Rasters used to leave the database to be analysed and come back as a shapefile. In a Rust engine that installs with `pip`, they stay: `RS_FromPath` reads them where they are, `RS_Clip` and `RS_ReprojectMatch` put them on a common grid, NumPy does the band math without a copy, `RS_Polygonize` turns the answer into geometry, `RS_ZonalStats` summarizes it by any polygon, and `RS_AsGeoTiff` writes it out. One session, one machine, one language.

*SedonaDB reference: [`RS_FromPath`](https://sedona.apache.org/sedonadb/latest/reference/sql/rs_frompath/), [`RS_ReprojectMatch`](https://sedona.apache.org/sedonadb/latest/reference/sql/rs_reprojectmatch/), [`RS_Polygonize`](https://sedona.apache.org/sedonadb/latest/reference/sql/rs_polygonize/), [`RS_ZonalStats`](https://sedona.apache.org/sedonadb/latest/reference/sql/rs_zonalstats/). Imagery © Planet Labs PBC, Crisis Response Program, CC BY-NC 4.0. Land cover © ESA WorldCover project 2021, contains modified Copernicus Sentinel data (2021). Commune boundaries from Overture Maps.*
