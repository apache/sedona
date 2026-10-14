---
date:
  created: 2026-10-13
links:
  - SedonaDB: https://sedona.apache.org/sedonadb/
authors:
  - dewey
  - feng
  - james
  - jia
title: "SedonaDB 0.5.0 Release"
---

# SedonaDB 0.5.0 Release


The Apache Sedona community is excited to announce the release of [SedonaDB](https://sedona.apache.org/sedonadb) version 0.5.0!

SedonaDB is the first open-source, single-node analytical database engine that treats spatial data as a first-class citizen. It is developed as a subproject of Apache Sedona. This post also highlights several changes from [SedonaDB 0.4.1](https://sedona.apache.org/sedonadb/latest/release-notes/#sedonadb-041), which did not receive a separate release post.

Apache Sedona powers large-scale geospatial processing on distributed engines like Spark (SedonaSpark), Flink (SedonaFlink), and Snowflake (SedonaSnow). SedonaDB extends the Sedona ecosystem with a single-node engine optimized for small-to-medium data analytics, delivering simplicity and speed for spatial data that distributed systems and general-purpose databases cannot.

<!-- more -->

## Release Highlights

The SedonaDB 0.5.0 release features a number of exciting updates!

- FileFormat reorganization
- Python support for `DataFrame.unnest()`
- Improved raster functionality
- GeoPandas-compatible API coverage
- Improved spatial function coverage
- Extensible table providers

As usual, Python users can install from PyPI:


```python
# pip install --upgrade "apache-sedona[db]"
import sedona.db

sd = sedona.db.connect()
sd.options.interactive = True
f = sd.funcs
```

R users can install from [R-multiverse](https://r-multiverse.org):

```r
install.packages(
  "sdplyr",
  repos = c("https://community.r-multiverse.org", "https://cloud.r-project.org")
)
```

We'll use a few example datasets from [geoarrow.org's data section](https://geoarrow.org/data):


```python
cities_url = "https://raw.githubusercontent.com/geoarrow/geoarrow-data/v0.2.0/natural-earth/files/natural-earth_cities.parquet"
countries_url = "https://raw.githubusercontent.com/geoarrow/geoarrow-data/v0.2.0/natural-earth/files/natural-earth_countries.parquet"
```

## FileFormat reorganization

Previous versions of SedonaDB could read popular spatial file formats such as [FlatGeobuf](https://flatgeobuf.org), Shapefile, and [GeoPackage](https://www.geopackage.org/) in addition to GeoParquet and Parquet. However, SQL, Python, and R used slightly different code paths, and formats such as CSV and JSON were supported in SQL but not in Python or R. In 0.5.0, all reads go through the same pathway for better consistency among bindings. For example, all of the following access the same dataset through the unified format machinery:


```python
sd.read(cities_url)
sd.read(cities_url, format="parquet")
sd.read.parquet(cities_url)
sd.sql(f"SELECT * FROM '{cities_url}'")

sd.sql(f"CREATE EXTERNAL TABLE cities STORED AS PARQUET LOCATION '{cities_url}'")
sd.sql("SELECT * FROM cities")

# In R:
# sd_read(cities_url)
# sd_read(cities_url, format = "parquet")
```




    ┌──────────────┬───────────────────────────────┐
    │     name     ┆            geometry           │
    │     utf8     ┆            geometry           │
    ╞══════════════╪═══════════════════════════════╡
    │ Vatican City ┆ POINT(12.4533865 41.9032822)  │
    ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    │ San Marino   ┆ POINT(12.4417702 43.9360958)  │
    ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    │ Vaduz        ┆ POINT(9.5166695 47.1337238)   │
    ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    │ Lobamba      ┆ POINT(31.1999971 -26.4666675) │
    ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    │ Luxembourg   ┆ POINT(6.1300028 49.6116604)   │
    ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    │ Palikir      ┆ POINT(158.1499743 6.9166437)  │
    ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    │ Majuro       ┆ POINT(171.3800002 7.1030043)  │
    ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    │ Funafuti     ┆ POINT(179.2166471 -8.516652)  │
    ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    │ Melekeok     ┆ POINT(134.6265485 7.4873962)  │
    ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    │ Bir Lehlou   ┆ POINT(-9.6525222 26.1191667)  │
    └──────────────┴───────────────────────────────┘



CSV and JSON reads are now better supported: `sd.read()` (or `sd_read()` in R) should "just work" for most common data sources.

## Python support for `DataFrame.unnest()`

One of my favorite functions in R has always been tidyr's `unnest_longer()`, which in SQL is an `UNNEST` and in Python has a few names depending on which DataFrame API you're dealing with. SedonaDB's version is `unnest()`, which works nicely with `ST_Dump()` to separate multigeometries and geometry collections into one row per component geometry.


```python
countries = sd.read.parquet(countries_url)

country_parts = (
    countries.mutate(parts=countries.geometry.geo.dump())
    .select("name", "continent", "parts")
    .unnest("parts")
    .select("name", "continent", geometry=sd.col("parts")["geom"])
    .rename(country="name")
)

(
    country_parts.group_by("continent")
    .agg(parts=f.count(sd.col("geometry")))
    .sort(sd.col("parts").desc())
)
```




    ┌─────────────────────────┬───────┐
    │        continent        ┆ parts │
    │           utf8          ┆ int64 │
    ╞═════════════════════════╪═══════╡
    │ Asia                    ┆    73 │
    ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
    │ Europe                  ┆    62 │
    ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
    │ North America           ┆    58 │
    ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
    │ Africa                  ┆    52 │
    ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
    │ Oceania                 ┆    19 │
    ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
    │ South America           ┆    15 │
    ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
    │ Antarctica              ┆     8 │
    ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
    │ Seven seas (open ocean) ┆     1 │
    └─────────────────────────┴───────┘



## Improved raster functionality

In our [last release post](https://sedona.apache.org/latest/blog/2026/06/19/sedonadb-040-release/) we warned that we're not quite ready to announce raster support in SedonaDB. In 0.5.0, we're still not quite ready to make a splash; however, we've made substantial progress in the underlying plumbing so that one can now solve a few real-world problems. For example, you can now pass a NumPy array with bounds into a SedonaDB Raster query without copying any pixel data (in this case, using `RS_Value()` to sample it with an existing geometry column).


```python
import numpy as np
from sedonadb.raster import Raster

pixels = np.arange(256 * 256, dtype="float32").reshape(256, 256)
raster = Raster.from_numpy(
    pixels,
    bbox=[-180, -90, 180, 90],
    crs="OGC:CRS84",
)

t = sd.read(cities_url)
t.mutate(raster_value=f.rst.value(raster, t.geometry)).sort("name").limit(5)
```




    ┌─────────────┬──────────────────────────────────────────────┬──────────────┐
    │     name    ┆                   geometry                   ┆ raster_value │
    │     utf8    ┆                   geometry                   ┆    float64   │
    ╞═════════════╪══════════════════════════════════════════════╪══════════════╡
    │ Abidjan     ┆ POINT(-4.020206835187587 5.3231260722445715) ┆      30845.0 │
    ├╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    │ Abu Dhabi   ┆ POINT(54.3665934 24.4666836)                 ┆      23974.0 │
    ├╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    │ Abuja       ┆ POINT(7.489505042885861 9.054620406360845)   ┆      29573.0 │
    ├╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    │ Accra       ┆ POINT(-0.2186616 5.5519805)                  ┆      30847.0 │
    ├╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    │ Addis Ababa ┆ POINT(38.6980586 9.0352562)                  ┆      29595.0 │
    └─────────────┴──────────────────────────────────────────────┴──────────────┘



We've also implemented raster/vector joins à la Sedona Spark so that interactions between rasters, N-D arrays, and vector features can be expressed in a SedonaDB logical plan. For example, calculating the mean annual number of dry days across raster cells in each country can now be expressed with:


```python
# pip install sedonadb-zarr sedonadb-expr
import sedonadb_zarr

sd.register(sedonadb_zarr.ZarrExtension())
era5_url = "https://weathermapdata.rdrn.me/era5_2015_2020_l5.zarr/2"
era5 = sd.read(era5_url, format="zarr", options={"arrays": ["rain_ok"]}).alias("era5")
countries = countries.alias("countries")

country_chunk_stats = (
    era5.join(
        countries,
        on=f.rs_intersects(era5.raster, countries.geometry),
    )
    .select(
        country=countries.name,
        stats=(
            era5.raster.rst.slice("year", 0).rst.zonal_stats_all(countries.geometry)
        ),
    )
)

country_totals = (
    country_chunk_stats.group_by("country")
    .agg(
        dry_day_sum=f.sum(sd.col("stats")["sum"]),
        pixel_years=f.sum(sd.col("stats")["count"]),
    )
    .filter(sd.col("pixel_years") > 0)
)

(
    country_totals.mutate(
        mean_dry_days=(
            country_totals.dry_day_sum / country_totals.pixel_years
        )
    ).sort(sd.col("mean_dry_days").desc())
)
```




    ┌──────────────────────┬────────────────────┬─────────────┬────────────────────┐
    │        country       ┆     dry_day_sum    ┆ pixel_years ┆    mean_dry_days   │
    │         utf8         ┆       float64      ┆    int64    ┆       float64      │
    ╞══════════════════════╪════════════════════╪═════════════╪════════════════════╡
    │ Egypt                ┆  430253.7284851074 ┆        1188 ┆  362.1664381187773 │
    ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    │ Libya                ┆  743429.1168518066 ┆        2064 ┆  360.1885256064955 │
    ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    │ Oman                 ┆ 124953.25396728516 ┆         348 ┆ 359.06107461863553 │
    ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    │ United Arab Emirates ┆ 30144.409088134766 ┆          84 ┆  358.8620129539853 │
    ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    │ Western Sahara       ┆  45196.26776123047 ┆         126 ┆  358.7005377875434 │
    ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    │ Mauritania           ┆  412543.5363769531 ┆        1164 ┆ 354.41884568466764 │
    ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    │ Qatar                ┆  4250.211212158203 ┆          12 ┆  354.1842676798503 │
    ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    │ Saudi Arabia         ┆  800630.7185974121 ┆        2268 ┆ 353.01178068668963 │
    ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    │ Algeria              ┆  1033613.354095459 ┆        2946 ┆  350.8531412408211 │
    ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    │ Niger                ┆ 448227.84048461914 ┆        1290 ┆ 347.46344223613886 │
    └──────────────────────┴────────────────────┴─────────────┴────────────────────┘



This version of a zonal statistics calculation is verbose compared to dedicated one-off tools. As with the 0.4.0 release, raster and N-D array support remains a work in progress, and we're working toward a balance of flexibility, performance, and familiarity with existing interfaces such as rasterio, the GDAL command-line interface, and xarray.

## GeoPandas-compatible API coverage

Over the past few releases, our fearless leader and PMC chair, [Jia Yu](https://github.com/jiayuasu), has been working toward a [GeoPandas](https://geopandas.org)-compatible API to help GeoPandas users get started with SedonaDB. The compatibility layer is still experimental but supports a number of common GeoPandas operations.


```python
# pip install sedonadb-geopandas
import sedonadb_geopandas as sgpd

countries = sgpd.GeoDataFrame(sd.read(countries_url)).to_crs("EPSG:6933")
countries = countries[["continent", "geometry"]]
countries["geometry"] = countries.geometry.buffer(100_000)
(
    countries.dissolve(by="continent")
    .to_crs("EPSG:4326")
    .to_geopandas()
)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>continent</th>
      <th>geometry</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Antarctica</td>
      <td>POLYGON ((178.96358 -84.71338, 178.96358 -89.9...</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Seven seas (open ocean)</td>
      <td>POLYGON ((67.69803 -49.04463, 67.68511 -49.307...</td>
    </tr>
    <tr>
      <th>2</th>
      <td>North America</td>
      <td>MULTIPOLYGON (((-82.20716 15.3375, -82.19334 1...</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Oceania</td>
      <td>MULTIPOLYGON (((165.69036 -45.17158, 166.22763...</td>
    </tr>
    <tr>
      <th>4</th>
      <td>South America</td>
      <td>MULTIPOLYGON (((-55.83725 -35.93421, -55.75148...</td>
    </tr>
    <tr>
      <th>5</th>
      <td>Africa</td>
      <td>MULTIPOLYGON (((-9.54423 4.16109, -9.77266 4.3...</td>
    </tr>
    <tr>
      <th>6</th>
      <td>Europe</td>
      <td>MULTIPOLYGON (((-55.11998 3.54951, -55.32571 3...</td>
    </tr>
    <tr>
      <th>7</th>
      <td>Asia</td>
      <td>MULTIPOLYGON (((53.21883 15.78142, 53.20306 15...</td>
    </tr>
  </tbody>
</table>
</div>



Unlike GeoPandas, a `sedonadb_geopandas.GeoDataFrame` remains lazy and is materialized when required (e.g., on `.to_geopandas()`). This allows SedonaDB's optimizer to do its magic and transparently parallelize the computation; however, it does mean that row order is not guaranteed and operations that need that assumption are not supported.

In addition to providing a familiar interface for users, GeoPandas compatibility lets us run GeoPandas tests, which has helped us catch a number of bugs in the underlying implementation.

## Improved spatial function coverage

Since the 0.4.0 release, we have been fortunate to work with contributors to add 39 new `ST_` and `RS_` functions to our growing catalog. These include `rs_asgeotiff`, `rs_asraster`, `rs_bandisnodata`, `rs_clip`, `rs_fromgdalraster`, `rs_geotransform`, `rs_makeemptyraster`, `rs_minconvexhull`, `rs_polygonize`, `rs_replacebandnodatavalue`, `rs_reprojectmatch`, `rs_resample`, `rs_setbandnodatavalue`, `rs_setgeoreference`, `rs_setvalue`, `rs_stack`, `rs_summarystats`, `rs_summarystatsall`, `rs_tile`, `rs_value`, `rs_values`, `rs_zonalstats`, `rs_zonalstatsall`, `st_buildarea`, `st_convexhull_agg`, `st_delaunaytriangles`, `st_exteriorring`, `st_geohash`, `st_geomcollfromtext`, `st_hausdorffdistance`, `st_linefromtext`, `st_linestringfromwkb`, `st_makeenvelope`, `st_mlinefromtext`, `st_mpointfromtext`, `st_mpolyfromtext`, `st_pointfromtext`, `st_pointfromwkb`, and `st_polygonfromtext`. This brings the SQL reference to 210 `ST_`, `RS_`, and `S2_` function pages, which are tested against PostGIS, SedonaSpark, and/or BigQuery for compatibility wherever possible. Check them out in our continuously improving [SQL reference documentation](https://sedona.apache.org/sedonadb/latest/reference/sql/)!

## Extensible table providers

Over the last few releases, we've been working on our extensibility layer so that less commonly used features can live in separate Python or R packages and contributors can maintain features in dedicated workspaces. The latest additions provide the machinery required to implement table providers, scans, and asynchronous streams across library boundaries. This work lays the foundation for integrations such as [Apache Iceberg](https://iceberg.apache.org/) and custom catalog hierarchies.

## What's Next?

As always, we're busy fixing bugs and working on new features for the next release. Stay tuned for new readers, improvements to existing readers, improvements to raster performance and syntax, and foreign catalog support.

## Contributors

```shell
$ git shortlog -sn apache-sedona-db-0.5.0.dev..apache-sedona-db-0.5.0
   114  James Willis
    73  Dewey Dunnington
    28  Jia Yu
     6  Hiroaki Yutani
     6  ajaypadwal73
     2  Kristin Cowalcijk
     1  Adon Metcalfe
     1  Jeroen Ooms
     1  Pranav Walimbe
```
