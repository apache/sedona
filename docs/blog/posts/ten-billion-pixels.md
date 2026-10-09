---
date:
  created: 2026-10-09
links:
  - RS_Tile in SedonaDB: https://sedona.apache.org/sedonadb/latest/reference/sql/rs_tile/
  - RS_ZonalStats in SedonaDB: https://sedona.apache.org/sedonadb/latest/reference/sql/rs_zonalstats/
  - ESA WorldCover: https://esa-worldcover.org/
authors:
  - jia
title: "Ten Billion Pixels, One SQL Statement"
slug: ten-billion-pixels-one-sql-statement
---

# Ten Billion Pixels, One SQL Statement

Where are the trees in Washington State? ESA mapped the land cover of the whole planet in 2021, one byte per 10 meter pixel. Washington is covered by 8 files of that map: 10.4 billion pixels, 9.7 GiB once uncompressed. SedonaDB 0.5.0 turns those files into a table and answers the question for all 39 counties in one SQL statement, in under a minute, on one machine.

![Land cover pixels over Puget Sound: dark green tree cover, red built-up land around Seattle and Tacoma, blue water, with white county lines](ten-billion-pixels-cover.png)

<!-- more -->

## One file is a billion pixels

Each WorldCover file covers 3 by 3 degrees at 10 meters: 36,000 by 36,000 pixels, one byte each, 1.3 billion pixels. Eight of them cover Washington, plus parts of Oregon, Idaho and British Columbia. The files are [public](https://registry.opendata.aws/esa-worldcover-vito/); the 8 together are 0.44 GiB to download.

`RS_FromPath` opens a file and reads the header. No pixel is read until a function needs one.

```python
import sedona.db

sd = sedona.db.connect()
sd.sql("""
SELECT RS_Width(r) AS width, RS_Height(r) AS height, RS_BandPixelType(r, 1) AS pixel_type,
       RS_BandNoDataValue(r, 1) AS nodata, RS_SRID(r) AS srid
FROM (SELECT RS_FromPath('ESA_WorldCover_10m_2021_v200_N45W123_Map.tif') AS r)
""").show()
```

```
┌───────┬────────┬────────────────┬─────────┬──────┐
│ width ┆ height ┆   pixel_type   ┆  nodata ┆ srid │
╞═══════╪════════╪════════════════╪═════════╪══════╡
│ 36000 ┆  36000 ┆ UNSIGNED_8BITS ┆     0.0 ┆ 4326 │
└───────┴────────┴────────────────┴─────────┴──────┘
```

The pixel values are class codes: 10 is tree cover, 50 is built-up, 80 is permanent water, and 0 means no data.

## The raster becomes a table

A file with a billion pixels is one row. `RS_Tile` cuts it into tiles of 2,048 by 2,048 pixels and returns one list per file; `unnest` turns the list into rows. Eight files become 2,592 rows, each holding a 4 MiB tile.

From there, every raster function in SedonaDB works per row. This statement counts, for every tile, how many pixels are tree cover:

```sql
WITH files AS (SELECT RS_FromPath(path) AS r FROM (VALUES ('N45W123.tif'), ('N48W123.tif')) AS f(path)),
tiles AS (SELECT t['x'] AS x, t['y'] AS y, t['tile'] AS tile
          FROM (SELECT unnest(RS_Tile(r, 2048, 2048)) AS t FROM files))
SELECT x, y,
       RS_SummaryStats(tile, 'count', 1, false) AS pixels,
       RS_SummaryStats(tile, 'count', 1) AS classified,
       RS_SummaryStats(tile, 'count', 1, false) - RS_SummaryStats(RS_SetBandNoDataValue(tile, 1, 10), 'count', 1) AS tree
FROM tiles
```

The last line uses a small trick. `RS_SummaryStats` with `count` counts the pixels that are not nodata, and the `false` at the end counts every pixel. `RS_SetBandNoDataValue` changes which value counts as nodata, without touching a pixel. Set it to 10, count again, and the difference from the full count is the number of tree pixels.

![Map of 2,592 tiles over Washington and its neighbors, each colored by its share of tree cover from pale to dark green, with the 39 county outlines in black](ten-billion-pixels-tiles.svg)

Over all 8 files that is 2,592 rows and 10.4 billion pixels. The scan behind the map also counts built-up and water pixels per tile, and it takes 87 seconds on ten cores.

## The county answer

Counties do not follow tile edges, so the county numbers need two more functions. `RS_Intersects` joins each tile to the counties it touches. `RS_ZonalStats` counts the pixels of a tile whose centers fall inside a county polygon. `SUM` adds the tiles up. The whole thing is one statement:

??? example "Land cover per county, one statement"

    ```sql
    WITH files AS (SELECT RS_FromPath(path) AS r FROM (VALUES ('N45W126.tif'), ('N45W123.tif'), ...) AS f(path)),
    tiles AS (SELECT t['tile'] AS tile FROM (SELECT unnest(RS_Tile(r, 2048, 2048)) AS t FROM files)),
    pairs AS (
      SELECT c.name,
             RS_ZonalStats(t.tile, c.geom, 1, 'count', false, false) AS pixels,
             RS_ZonalStats(t.tile, c.geom, 1, 'count') AS classified,
             RS_ZonalStats(RS_SetBandNoDataValue(t.tile, 1, 10), c.geom, 1, 'count') AS not_tree,
             RS_ZonalStats(RS_SetBandNoDataValue(t.tile, 1, 50), c.geom, 1, 'count') AS not_built,
             RS_ZonalStats(RS_SetBandNoDataValue(t.tile, 1, 80), c.geom, 1, 'count') AS not_water
      FROM tiles t JOIN counties c ON RS_Intersects(t.tile, c.geom))
    SELECT name, SUM(classified) AS pixels,
           SUM(pixels - not_tree) / SUM(classified) AS tree,
           SUM(pixels - not_built) / SUM(classified) AS built,
           SUM(pixels - not_water) / SUM(classified) AS water
    FROM pairs GROUP BY name ORDER BY tree DESC
    ```

    `counties` is a view over the Census Bureau's county boundaries, read with `sd.read_pyogrio` and transformed to EPSG:4326 to match the rasters. In the first count, the last `false` keeps nodata pixels in the count; the `false` before it is `all_touched`, left at its default.

The join produces 1,263 tile and county pairs, with 3.0 billion pixels inside Washington's counties. The statement took between 41 and 64 seconds across nine runs on ten cores.

| County | Pixels | Tree cover | Built-up | Water |
|---|---:|---:|---:|---:|
| Grays Harbor | 85,641,759 | 91.6% | 0.5% | 1.3% |
| Clallam | 79,522,899 | 90.5% | 0.5% | 1.8% |
| Skamania | 73,075,423 | 90.3% | 0.1% | 1.6% |
| King | 97,560,956 | 79.3% | 7.4% | 3.2% |
| Whitman | 95,895,183 | 2.0% | 0.6% | 0.9% |
| Adams | 85,117,674 | 0.5% | 0.8% | 0.3% |

Statewide, 52.0% of the classified pixels are tree cover, 1.35% are built-up and 1.84% are water. King County, with Seattle, is the most built-up county at 7.4%, and San Juan County has the most water at 10.3%.

The counts match NumPy. For one tile of King County, `bincount` over the clipped pixels gives the same 1,288,514 pixels, 654,652 of them trees, 529,781 built-up and 43,094 water.

## What it costs

![Resident memory over time for three runs over the same 8 files: the tile scan on one core stays near 4 GiB, the tile scan on ten cores reaches 12.5 GiB, and the county join reaches 12.8 GiB](ten-billion-pixels-memory.svg)

Memory follows a simple rule in the tile scan: the process holds one file's band and its tiles, about 4 GiB, and moves on to the next file. On one core that is the whole story. 10.4 billion pixels went by in 414 seconds and resident memory never passed 4.0 GiB. On ten cores several files are open at once, so the peak grows with the number of cores: 12.5 GiB in 87 seconds.

The county join holds more, 13 to 16 GiB across runs, because a tile stays in memory until the join has matched it with its counties. Running the join on one core did not change that; it only took 73 seconds instead of 45.

One setting from 0.5.0 sits under the one-file rule. Query engines size a batch of rows by row count, and a row that holds a raster can weigh a gigabyte. `sedona.raster.max_batch_bytes` sizes each batch of rasters by the pixel bytes it will hold, 256 MiB by default. The eight file rows arrive as one batch, and the budget splits it so that one band is loaded at a time. With the budget raised to 64 GiB, tiling the same 8 files on one core peaked at 12.9 GiB instead of 6.9 GiB.

## The point

- A big raster is a table of tiles. `RS_Tile` makes the rows, and SQL does the rest.
- Count a class by making it nodata and counting the others.
- When you need every pixel, scan tiles: memory stays at one file. When you need boundaries, join tiles to polygons, and expect the join to hold its tiles.

*Land cover from ESA WorldCover 2021 v200, CC BY 4.0; contains modified Copernicus Sentinel data (2021) processed by the ESA WorldCover consortium. County boundaries from the US Census Bureau, 2023 cartographic boundary files. Measurements on SedonaDB 0.5.0, one machine with 10 cores and 64 GiB.*
