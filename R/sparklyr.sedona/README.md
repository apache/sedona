# sparklyr.sedona

## Overview

Sparklyr.sedona is a [sparklyr](https://github.com/sparklyr/sparklyr) extension that aims to be an up-to-date R interface for [Apache Sedona](https://sedona.apache.org).
Apache Sedona provides a versatile set of Spatial [Resilient Distributed Datasets](https://spark.apache.org/docs/latest/rdd-programming-guide.html) (RDD) functionalities
and various Spark [UDT](https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/types/SQLUserDefinedType.html)s and
[UDF](https://spark.apache.org/docs/latest/sql-ref-functions-udf-scalar.html)s, enabling large-scale geospatial data to be processed and visualized in scalable and
efficient ways using Apache Spark.
Sparklyr.sedona, built on top of sparklyr, presents what Apache Sedona has to offer through idiomatic frameworks and constructs in R
(e.g., one can build spatial Spark SQL queries using Sedona UDFs in conjunction with a wide range of dplyr expressions), hence making Apache Sedona highly R-user friendly.

## Connecting to Spark

To ensure Sedona serialization routines, UDTs, and UDFs are properly registered when creating a Spark session, one simply needs to attach `sparklyr.sedona` before
instantiating a Spark conneciton. Sparklyr.sedona will take care of the rest. For example,

``` r
library(sparklyr)
library(sparklyr.sedona)

spark_home <- "/usr/lib/spark"  # NOTE: replace this with your $SPARK_HOME directory
sc <- spark_connect(master = "yarn", spark_home = spark_home)
```

will create a Sedona-capable Spark connection in YARN client mode, and

``` r
library(sparklyr)
library(sparklyr.sedona)

sc <- spark_connect(master = "local")
```

will create a Sedona-capable Spark connection to an Apache Spark instance running locally.

In `sparklyr`, one can easily inspect the Spark connection object to sanity-check it has been properly initialized with all Sedona-related dependencies, e.g.,

``` r
print(sc$extensions$packages)
```

```
## [1] "org.apache.sedona:sedona-core-3.0_2.12:1.0.0-incubating"
## [2] "org.apache.sedona:sedona-sql-3.0_2.12:1.0.0-incubating"
## [3] "org.apache.sedona:sedona-viz-3.0_2.12:1.0.0-incubating"
## [4] "org.datasyslab:geotools-wrapper:geotools-24.0"
## [5] "org.datasyslab:sernetcdf:0.1.0"
## [6] "org.locationtech.jts:jts-core:1.18.0"
## [7] "org.wololo:jts2geojson:0.14.3"
```

and

``` r
spark_session(sc) %>%
  invoke("%>%", list("conf"), list("get", "spark.kryo.registrator")) %>%
  print()
```

```
## [1] "org.apache.sedona.viz.core.Serde.SedonaVizKryoRegistrator"
```
.

For more information about connecting to Spark with `sparklyr`, see https://therinspark.com/connections.html and `?sparklyr::spark_connect`.
Also see https://sedona.apache.org/tutorial/rdd/#initiate-sparkcontext for minimum and recommended dependencies for Apache Sedona.

## What are `SpatialRDD`s?

[SpatialRDD](https://sedona.apache.org/api/javadoc/core/org/apache/sedona/core/spatialRDD/SpatialRDD.html)s are basic building blocks of
distributed spatial data in Apache Sedona. A `SpatialRDD` can be spatially partitioned and indexed to facilitate range queries, KNN queries,
and other low-level operations. One can also export records from `SpatailRDD`s into regular Spark dataframes, making them accessible through
Spark SQL and through the `dplyr` interface of `sparklyr` (more on that later).

## Creating a SpatialRDD

NOTE: this section is largely based on https://sedona.apache.org/tutorial/rdd/#create-a-spatialrdd, except for examples have been
written in R instead of Scala to reflect usages of `sparklyr.sedona`.

Currently `SpatialRDD`s can be created in `sparklyr.sedona` by reading a file in a supported geospatial format, or by extracting data from a
Spark SQL query.

For example, the following code will import data from [arealm-small.csv](https://github.com/apache/incubator-sedona/blob/master/binder/data/arealm-small.csv) into a `SpatialRDD`:

``` r
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

Records from the example [arealm-small.csv](https://github.com/apache/incubator-sedona/blob/master/binder/data/arealm-small.csv) file look like the following:

```
testattribute0,-88.331492,32.324142,testattribute1,testattribute2
testattribute0,-88.175933,32.360763,testattribute1,testattribute2
testattribute0,-88.388954,32.357073,testattribute1,testattribute2
```

As one can see from the above, each record is comma-separated and consists of a 2-dimensional coordinate starting at the 2nd column and ending at the 3rd column.
All other columns contain non-spatial attributes. Because column indexes are 0-based, we need to specify `first_spatial_col_index = 1` in the example above to
ensure each record is parsed correctly.

In addition to formats such as CSV and TSV, currently `sparklyr.sedona` also supports reading files in WKT (Well-Known Text), WKB (Well-Known Binary), and GeoJSON formats.
See `?sparklyr.sedona::sedona_read_wkt`, `?sparklyr.sedona::sedona_read_wkb`, and `?sparklyr.sedona::sedona_read_geojson` for details.

One can also run `to_spatial_rdd()` to extract a SpatailRDD from a Spark SQL query, e.g.,

``` r
library(sparklyr)
library(sparklyr.sedona)
library(dplyr)

sc <- spark_connect(master = "local")

sdf <- tbl(
  sc,
  sql("SELECT ST_GeomFromText('POINT(-71.064544 42.28787)') AS `geom`, \"point\" AS `type`")
)

spatial_rdd <- sdf %>% to_spatial_rdd(spatial_col = "geom")
print(spatial_rdd)
```

```
## $.jobj
## <jobj[70]>
##   org.apache.sedona.core.spatialRDD.SpatialRDD
##   org.apache.sedona.core.spatialRDD.SpatialRDD@422afc5a
##
## ...
```

will extract a spatial column named `"geom"` from the Sedona spatial SQL query above and store it in a `SpatialRDD` object.

## Working with Spark dataframes

As mentioned previously, data from `SpatialRDD` can be exported into a Spark dataframe and be queried and modified through
the `dplyr` interface of `sparklyr`. The example below shows how `sdf_register()`, a S3 generic that converts a lower-level
object into a Spark dataframe object in `sparklyr`, can be applied to a `SpatialRDD` object created by `sparklyr.sedona`.

``` r
library(sparklyr)
library(sparklyr.sedona)

sc <- spark_connect(master = "local")
polygon_rdd <- sedona_read_geojson(sc, location = "/tmp/polygon.json")
polygon_sdf <- polygon_rdd %>% sdf_register()

polygon_sdf %>% print(n = 3)
```

```
## # Source: spark<?> [?? x 1]
##   geometry
##   <list>
## 1 <POLYGON ((-87.621765 34.873444, -87.617535 34.873369, -87.6123 34.873337, -8…
## 2 <POLYGON ((-85.719017 31.297901, -85.715626 31.305203, -85.714271 31.307096, …
## 3 <POLYGON ((-86.000685 34.00537, -85.998837 34.009768, -85.998012 34.010398, -…
## # … with more rows
```

The Spark dataframe object can then be modified using `dplyr` verbs familiar to many R users. In addition, spatial UDFs
supported by Sedona can inter-operate seamlessly with other functions supported in `sparklyr`'s dbplyr SQL translation
env. For example, the code below finds the average area of all polygons in `polygon_sdf`:

``` r
mean_area_sdf <- polygon_sdf %>%
  dplyr::summarize(dplyr::summarize(mean_area = mean(ST_Area(geometry))))
print(mean_area_sdf)
```

```
## # Source: spark<?> [?? x 1]
##   mean_area
##       <dbl>
## 1   0.00217
```

Once spatial objects are imported into Spark dataframes, they can also be easily integrated with other non-spatial attributes,
e.g.,

``` r
modified_polygon_sdf <- polygon_sdf %>%
  dplyr::mutate(type = "polygon")
```
.

Notice all of the above can open up many interesting possiblities. For example, one can extract ML features from geospatial
data in Spark dataframes, build a ML pipeline using `ml_*` family of functions in `sparklyr` to work with such features, and if
the output of a ML model happens to be a geospatial object as well, one can even apply visualization routines in
`sparklyr.sedona` to visualize the difference between any predicted geometry and the corresponding ground truth
(more on visualization later).

## Visualization

It is worth mentioning `sparklyr.sedona` also implements R interfaces to Sedona visualization routines. For example, the following
is essentially the R equivalent of [this example in Scala](https://github.com/apache/incubator-sedona/blob/f6b1c5e24bdb67d2c8d701a9b2af1fb5658fdc4d/viz/src/main/scala/org/apache/sedona/viz/showcase/ScalaExample.scala#L142-L160).

``` r
library(sparklyr)
library(sparklyr.sedona)

sc <- spark_connect(master = "local")

resolution_x <- 1000
resolution_y <- 600
boundary <- c(-126.790180, -64.630926, 24.863836, 50.000)

pt_rdd <- sedona_read_dsv_to_typed_rdd(
  sc,
  location = "arealm.csv",
  type = "point"
)
polygon_rdd <- sedona_read_dsv_to_typed_rdd(
  sc,
  location = "primaryroads-polygon.csv",
  type = "polygon"
)
pair_rdd <- sedona_spatial_join_count_by_key(
  pt_rdd,
  polygon_rdd,
  join_type = "intersect"
)

overlay <- sedona_render_scatter_plot(
  polygon_rdd,
  resolution_x,
  resolution_y,
  output_location = tempfile("scatter-plot-"),
  boundary = boundary,
  base_color = c(255, 0, 0),
  browse = FALSE
)

sedona_render_choropleth_map(
  pair_rdd,
  resolution_x,
  resolution_y,
  output_location = "/tmp/choropleth-map",
  boundary = boundary,
  overlay = overlay,
  # vary the green color channel according to relative magnitudes of data points so
  # that the resulting map will show light blue, light purple, and light gray pixels
  color_of_variation = "green",
  base_color = c(225, 225, 255)
)
```

It wlll create a scatter plot, and then overlay it on top of a choropleth map, as shown below:

<img src="docs/choropleth-map.png" width=800 />

See `?sparklyr.sedona::sedona_render_scatter_plot`, `?sparklyr.sedona::sedona_render_heatmap`,
and `?sparklyr.sedona::sedona_render_choropleth_map` for more details on R interfaces of
Sedona visualization routines currently implemented by `sparklyr.sedona`.
