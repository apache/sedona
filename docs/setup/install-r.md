## Introduction

apache.sedona ([cran.r-project.org/package=apache.sedona](https://cran.r-project.org/package=apache.sedona)) is a
[sparklyr](https://github.com/sparklyr/sparklyr)-based R interface for
[Apache Sedona](https://sedona.apache.org). It presents what Apache
Sedona has to offer through idiomatic frameworks and constructs in R
(e.g., one can build spatial Spark SQL queries using Sedona UDFs in
conjunction with a wide range of dplyr expressions), hence making Apache
Sedona highly friendly for R users.

Generally speaking, when working with Apache Sedona, one choose between
the following two modes:

-   Manipulating Sedona [Spatial Resilient Distributed
    Datasets](../../tutorial/rdd)
    with spatial-RDD-related routines
-   Querying geometric columns within [Spatial dataframes](../../tutorial/sql) with Sedona
    spatial UDFs

While the former option enables more fine-grained control over low-level
implementation details (e.g., which index to build for spatial queries,
which data structure to use for spatial partitioning, etc), the latter
is simpler and leads to a straightforward integration with `dplyr`,
`sparklyr`, and other `sparklyr` extensions (e.g., one can build ML
feature extractors with Sedona UDFs and connect them with ML pipelines
using `ml_*()` family of functions in `sparklyr`, hence creating ML
workflows capable of understanding spatial data).

Because data from spatial RDDs can be imported into Spark dataframes as
geometry columns and vice versa, one can switch between the
abovementioned two modes fairly easily.

At the moment `apache.sedona` consists of the following components:

-   R interface for Spatial-RDD-related functionalities
    -   Reading/writing spatial data in WKT, WKB, and GeoJSON formats
    -   Shapefile reader
    -   Spatial partition, index, join, KNN query, and range query
        operations
    -   Visualization routines
-   `dplyr`-integration for Sedona spatial UDTs and UDFs
    -   See [SQL APIs](../../api/sql/Overview/) for the list
        of available UDFs
-   Functions importing data from spatial RDDs to Spark dataframes and
    vice versa

## Connect to Spark

To ensure Sedona serialization routines, UDTs, and UDFs are properly
registered when creating a Spark session, one simply needs to attach
`apache.sedona` before instantiating a Spark connection. apache.sedona
will take care of the rest. For example,

``` r
library(sparklyr)
library(apache.sedona)

spark_home <- "/usr/lib/spark"  # NOTE: replace this with your $SPARK_HOME directory
sc <- spark_connect(master = "yarn", spark_home = spark_home)
```

will create a Sedona-capable Spark connection in YARN client mode, and

``` r
library(sparklyr)
library(apache.sedona)

sc <- spark_connect(master = "local")
```

will create a Sedona-capable Spark connection to an Apache Spark
instance running locally.

In `sparklyr`, one can easily inspect the Spark connection object to
sanity-check it has been properly initialized with all Sedona-related
dependencies, e.g.,

``` r
print(sc$extensions$packages)
```

    ## [1] "org.apache.sedona:sedona-core-3.0_2.12:{{ sedona.current_version }}"
    ## [2] "org.apache.sedona:sedona-sql-3.0_2.12:{{ sedona.current_version }}"
    ## [3] "org.apache.sedona:sedona-viz-3.0_2.12:{{ sedona.current_version }}"
    ## [4] "org.datasyslab:geotools-wrapper:{{ sedona.current_geotools }}"
    ## [5] "org.datasyslab:sernetcdf:0.1.0"
    ## [6] "org.locationtech.jts:jts-core:1.18.0"
    ## [7] "org.wololo:jts2geojson:0.14.3"

and

``` r
spark_session(sc) %>%
  invoke("%>%", list("conf"), list("get", "spark.kryo.registrator")) %>%
  print()
```

    ## [1] "org.apache.sedona.viz.core.Serde.SedonaVizKryoRegistrator"


For more information about connecting to Spark with `sparklyr`, see
<https://therinspark.com/connections.html> and
`?sparklyr::spark_connect`. Also see
[Initiate Spark Context](../../tutorial/rdd/#initiate-sparkcontext) and [Initiate Spark Session](../../tutorial/sql/#initiate-sparksession) for
minimum and recommended dependencies for Apache Sedona.
