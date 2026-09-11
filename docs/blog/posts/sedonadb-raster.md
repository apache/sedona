---
date:
  created: 2026-09-11
links:
  - SedonaDB quickstart: https://sedona.apache.org/sedonadb/latest/quickstart-python/
  - RS_Polygonize in SedonaDB: https://sedona.apache.org/sedonadb/latest/reference/sql/rs_polygonize/
  - Planet Crisis Response data: https://source.coop/planet/disasterdata
authors:
  - jia
title: "SELECT * FROM Satellite"
---

# SELECT * FROM Satellite

[SedonaDB](https://sedona.apache.org/sedonadb/latest/) 0.4.1 ships a raster layer: rasters stream from cloud storage without a download, they clip, resample and reproject in SQL, zonal statistics and polygonization are functions, and pixels move to NumPy and back without a copy. All of it runs on one machine after a `pip install`. The tutorial below puts every piece to work on one event: the Gironde and Landes wildfire of July 2026, which burned through the pine forest west of Bordeaux and prompted 250,000 evacuations. Eleven PlanetScope scenes go in, and burn perimeters, hectares per commune and a GeoTIFF come out.

![Left: title and three numbers. Right: PlanetScope true-color image of the Gironde coast on 28 July 2026 with the burn scar outlined in red polygons](sedonadb-raster-cover.png)

<!-- more -->

## A raster catalog is a table

Planet publishes its crisis imagery as public Cloud Optimized GeoTIFFs with a [STAC-GeoParquet](https://source.coop/planet/disasterdata) index per event. SedonaDB reads that index straight over HTTPS, and the footprints are a geometry column, so choosing scenes is a spatial query:

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
2    pre  20260708_105603_64_2544      0
3    pre  20260708_105606_00_2544      0
4    pre  20260708_105829_58_2558     18
5    pre  20260708_105827_24_2558     20
6    pre  20260708_105831_92_2558     32
7    pre  20260708_105834_26_2558     58
8   post  20260728_105820_44_2562      0
9   post  20260728_105825_11_2562      0
10  post  20260728_105822_78_2562      5
```

Eight scenes from 8 July, before the fire started on the 23rd, and three from 28 July, while it was still burning. Two seconds, and no pixel has been read yet.

## Rasters without downloads

`RS_FromPath` opens a file's header and returns an out-of-database raster: georeferencing in the value, bands pointing at the source, pixels fetched only when a function asks for them. A PlanetScope surface-reflectance scene is half a gigabyte; opening one takes about a second:

```python
sd.sql(f"""
    SELECT RS_Width(r) AS w, RS_Height(r) AS h, RS_NumBands(r) AS nb, RS_SRID(r) AS srid,
           RS_ScaleX(r) AS sx, RS_BandPixelType(r, 1) AS t, RS_BandNoDataValue(r, 1) AS nodata
    FROM (SELECT RS_FromPath('{scenes.href[8]}') AS r)
""").to_pandas().to_dict("records")[0]
```

```
{'w': 12715, 'h': 9085, 'nb': 4, 'srid': 32630, 'sx': 3.0, 't': 'UNSIGNED_16BITS', 'nodata': 0.0}
```

Four bands at 3 m in UTM zone 30. Every scene has its own origin, so the analysis needs one common grid. That grid is a raster too, built from a NumPy array of zeros at 12 m over the study area, and `RS_ReprojectMatch` puts every clip onto it. A raster made in Python enters SQL as a query parameter:

```python
import numpy as np
from sedonadb.raster import Raster

W, H = 2374, 3017  # 12 m cells over the 28 x 36 km study area, UTM 30N
GT = [633936.0, 12, 0, 4987224.0, 0, -12]
ref = Raster.from_numpy(np.zeros((H, W), np.uint8), crs="EPSG:32630", transform=GT)


def aligned(phase, band, algorithm="Average"):
    """One statement clips and aligns every scene of a date; SedonaDB runs the rows in parallel."""
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

`RS_Clip` reads only the window inside the study area, `RS_ReprojectMatch` averages the 3 m pixels into the 12 m cells, and `to_numpy()` hands each result to NumPy as a view. The scenes of a date are rows of one `VALUES` table, so one statement fans the reads out across cores. Red, near-infrared and the usable-data mask each take one statement per date, six in all, and a few lines of NumPy stack the scenes into one mosaic per date, first clear pixel wins. The six statements take eleven and a half minutes together, nearly all of it range requests into half-gigabyte files over HTTPS; the eleven scenes cover 87 percent of the grid, the rest is ocean and the gaps between strips.

## Band math in NumPy, back into SQL

The burn index is the drop in NDVI between the two dates. Otsu's method picks the cutoff from the data: it splits the histogram of that drop over pixels that were vegetated before the fire, and lands at 0.225:

```python
def ndvi(red, nir):
    return np.where(red + nir > 0, (nir - red) / np.maximum(red + nir, 1), np.nan)


drop = ndvi(pre_red, pre_nir) - ndvi(post_red, post_nir)
burn = (both_dates & (ndvi(pre_red, pre_nir) > 0.35) & (drop > 0.225)).astype(np.uint8)
mask = Raster.from_numpy(burn, crs="EPSG:32630", transform=GT)
```

`mask` is an in-database raster now, and the rest of the post is SQL against it.

## RS_Polygonize: the scar becomes geometry

`RS_Polygonize` returns one polygon per connected run of equal-valued pixels, with the value attached. `unnest` turns the list into rows, and from there every vector function applies:

```python
sd.sql(
    """
    SELECT p.geom AS geom, ST_Area(p.geom) AS m2
    FROM (SELECT unnest(RS_Polygonize($1, 1)) AS p)
    WHERE p.value = 1
""",
    params=(mask,),
).to_memtable().to_view("patches")

sd.sql("""
    SELECT ROUND(m2 / 1e4) AS ha, ST_AsText(ST_Centroid(ST_Transform(geom, 'EPSG:4326'))) AS c
    FROM patches ORDER BY m2 DESC LIMIT 5
""").to_pandas()
```

```
        ha                                             c
0  21090.0  POINT(-1.0499563746105551 44.85692261508244)
1    855.0  POINT(-1.1992262050284765 44.79970523209393)
2    303.0  POINT(-0.9643428713835945 44.76723745400395)
3    299.0   POINT(-1.184405957042549 44.78253924016307)
4    179.0  POINT(-1.1431500135573076 44.83571182348193)
```

Polygonizing the 7-million-cell mask takes 2.5 seconds and yields 3,769 patches. Dropping everything under a hectare leaves **173 polygons covering 24,091 ha**, and one of them is the fire: a single 21,090 ha geometry between Le Porge and Lanton.

![PlanetScope true-color image of the coast between Lacanau and the Arcachon Basin on 28 July 2026, the burn scar dark purple, outlined by 173 red polygons; smoke plumes still rise from the western edge](sedonadb-raster-burn.png)

## Zonal statistics on the mask

The same raster parameter works in `RS_ZonalStats`. The zones are the communes, read earlier from Overture's divisions theme, and the sum of a 0/1 mask over a polygon is a pixel count, so hectares are one multiplication away:

```python
per_commune = sd.sql(
    """
    SELECT name, RS_ZonalStats($1, geometry, 'sum') * 0.0144 AS burned_ha
    FROM communes ORDER BY burned_ha DESC
""",
    params=(mask,),
).to_pandas()
```

```
                  name  burned_ha
9             Le Porge  6202.1232
12              Saumos  4044.7440
7               Lanton  3498.6384
6                 Arès  3498.3792
10             Lacanau  2025.4464
8            Le Temple  1667.0736
11       Sainte-Hélène   236.1456
5   Andernos-les-Bains   222.8688
4              Audenge   109.4832
```

![Horizontal bar chart of burned hectares by commune: Le Porge 6,202, Saumos 4,045, Lanton 3,499, Arès 3,498, Lacanau 2,025, Le Temple 1,667, then three communes under 250](sedonadb-raster-communes.svg)

Twenty-seven communes in 12.5 seconds.

## Three rasters, one grid

Because the grid is a raster, any other raster can be aligned to it. ESA WorldCover, the same 10 m land-cover map used in [last week's post](https://sedona.apache.org/latest/blog/2026/09/04/group-by-but-for-pixels/), goes through the same `RS_Clip` and `RS_ReprojectMatch` pair with `NearestNeighbor` in place of `Average`, since classes must not be averaged, and a NumPy crosstab says what burned:

```
   land_cover  class  burned_ha  share_pct
   tree cover     10    18262.5       75.0
    grassland     30     5501.0       22.6
     cropland     40      548.7        2.3
     built-up     50       23.7        0.1
```

Three quarters of the scar is the Landes pine forest, most of the rest is the grassland and heath between the plantations. The 549 ha of cropland is a caveat worth reading off the map: a few harvested center-pivot fields at the southeast edge lost as much NDVI as a burned stand, and a vegetation-drop index cannot tell the two apart. The land-cover column is how that gets filtered when it matters.

## Write it back

`RS_AsGeoTiff` encodes a raster as bytes, tiled and compressed, and the polygons write out as GeoParquet with `to_parquet`:

```python
tif = sd.sql(
    "SELECT RS_AsGeoTiff($1, 'DEFLATE', 0.0, 256) AS tif", params=(mask,)
).to_arrow_table()
open("burn_mask.tif", "wb").write(tif["tif"][0].as_py())
sd.sql(
    "SELECT ST_Transform(geom, 'EPSG:4326') AS geom, m2 FROM patches WHERE m2 >= 10000"
).to_parquet("burn_patches.parquet")
```

The mask is a 137 KB GeoTIFF that any GIS opens on the study-area grid.

## What the numbers are and are not

The whole pipeline, from catalog query to GeoTIFF, ran in twelve minutes in one process, and all but thirty seconds of that was streaming pixels: everything after the reads, the mask, the polygonize, the zonal statistics and the export, takes under twenty seconds. The 24,091 ha is the scar visible on the morning of 28 July at 12 m, with the pixels under that morning's smoke plumes masked out by Planet's usable-data layer. The fire kept burning after those scenes were captured, and the estimates published for the whole complex run from 34,000 to 45,000 ha. Rerunning against a later scene is a change of one line in the catalog query.

## The point

Rasters used to leave the database to be analysed and come back as a shapefile. In SedonaDB 0.4.1 they stay: `RS_FromPath` reads them where they are, `RS_Clip` and `RS_ReprojectMatch` put them on a common grid, NumPy does the band math without a copy, `RS_Polygonize` turns the answer into geometry, `RS_ZonalStats` summarizes it by any polygon, and `RS_AsGeoTiff` writes it out. One session, one machine, one language.

*SedonaDB reference: [`RS_FromPath`](https://sedona.apache.org/sedonadb/latest/reference/sql/rs_frompath/), [`RS_ReprojectMatch`](https://sedona.apache.org/sedonadb/latest/reference/sql/rs_reprojectmatch/), [`RS_Polygonize`](https://sedona.apache.org/sedonadb/latest/reference/sql/rs_polygonize/), [`RS_ZonalStats`](https://sedona.apache.org/sedonadb/latest/reference/sql/rs_zonalstats/). Imagery © Planet Labs PBC, Crisis Response Program, CC BY-NC 4.0. Land cover © ESA WorldCover project 2021, contains modified Copernicus Sentinel data (2021). Commune boundaries from Overture Maps.*
