# Spatial RDD applications in R language

## What are `SpatialRDD`s?

[SpatialRDDs](../rdd) are basic building blocks of distributed spatial data in Apache Sedona.
A `SpatialRDD` can be partitioned and indexed using well-known spatial
data structures to facilitate range queries, KNN queries, and other
low-level operations. One can also export records from `SpatailRDD`s
into regular Spark dataframes, making them accessible through Spark SQL
and through the `dplyr` interface of `sparklyr`.

## Creating a SpatialRDD

NOTE: this section is largely based on
[Spatial RDD Scala tutorial](../rdd/#create-a-spatialrdd), except
for examples have been written in R instead of Scala to reflect usages
of `apache.sedona`.

Currently `SpatialRDD`s can be created in `apache.sedona` by reading a
file in a supported geospatial format, or by extracting data from a
Spark SQL query.

For example, the following code will import data from
[arealm-small.csv](https://github.com/apache/sedona/blob/master/binder/data/arealm-small.csv)
into a `SpatialRDD`:

```r
pt_rdd <- sedona_read_dsv_to_typed_rdd(
  sc,
  location = "arealm-small.csv",
  delimiter = ",",
  type = "point",
  first_spatial_col_index = 1,
  has_non_spatial_attrs = TRUE
)
```

.

Records from the example
[arealm-small.csv](https://github.com/apache/sedona/blob/master/binder/data/arealm-small.csv)
file look like the following:

    testattribute0,-88.331492,32.324142,testattribute1,testattribute2
    testattribute0,-88.175933,32.360763,testattribute1,testattribute2
    testattribute0,-88.388954,32.357073,testattribute1,testattribute2

As one can see from the above, each record is comma-separated and
consists of a 2-dimensional coordinate starting at the 2nd column and
ending at the 3rd column. All other columns contain non-spatial
attributes. Because column indexes are 0-based, we need to specify
`first_spatial_col_index = 1` in the example above to ensure each record
is parsed correctly.

In addition to formats such as CSV and TSV, currently `apache.sedona`
also supports reading files in WKT (Well-Known Text), WKB (Well-Known
Binary), and GeoJSON formats. See `?apache.sedona::sedona_read_wkt`,
`?apache.sedona::sedona_read_wkb`, and
`?apache.sedona::sedona_read_geojson` for details.

One can also run `to_spatial_rdd()` to extract a SpatailRDD from a Spark
SQL query, e.g.,

```r
library(sparklyr)
library(apache.sedona)
library(dplyr)

sc <- spark_connect(master = "local")

sdf <- tbl(
  sc,
  sql("SELECT ST_GeomFromText('POINT(-71.064544 42.28787)') AS `geom`, \"point\" AS `type`")
)

spatial_rdd <- sdf %>% to_spatial_rdd(spatial_col = "geom")
print(spatial_rdd)
```

    ## $.jobj
    ## <jobj[70]>
    ##   org.apache.sedona.core.spatialRDD.SpatialRDD
    ##   org.apache.sedona.core.spatialRDD.SpatialRDD@422afc5a
    ##
    ## ...

will extract a spatial column named `"geom"` from the Sedona spatial SQL
query above and store it in a `SpatialRDD` object.
