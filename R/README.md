

# apache.sedona <img src="man/figures/logo.png" align="right" width="120"/>
[Apache Sedona](https://sedona.apache.org/) is a cluster computing system for processing large-scale spatial data. Sedona extends existing cluster computing systems, such as Apache Spark and Apache Flink, with a set of out-of-the-box distributed Spatial Datasets and Spatial SQL that efficiently load, process, and analyze large-scale spatial data across machines.


The apache.sedona R package exposes an interface to Apache Sedona through `{sparklyr}`
enabling higher-level access through a `{dplyr}` backend and familiar R functions.


## Installation
To use Apache Sedona from R, you just need to install the apache.sedona package; Spark dependencies are managed directly by the package.

``` r
# Install released version from CRAN
install.packages("apache.sedona")
```

#### Development version
To use the development version, you will need both the latest version of the package and of the Apache Sedona jars.

To get the latest R package from GtiHub:

``` r
# Install development version from GitHub
devtools::install_github("apache/sedona/R")
```

To get the latest Sedona jars you can:

* **Compile the Sedona code yourself**, see [Compile the code](https://sedona.apache.org/latest-snapshot/setup/compile/)
* **Get the latest generated jars** from the [GitHub 'Java build' action](https://github.com/apache/sedona/actions/workflows/java.yml); click on the latest run, the generated jars are at the bottom of the page

The path to the sedona-spark-shaded and sedona-viz jars needs to be put in the `SEDONA_JAR_FILES` environment variables (see below).


## Usage

`spark_read_*` functions will read geospatial data into Spark Dataframes. The resulting Spark dataframe object can then be modified using dplyr verbs familiar to many R users. In addition, spatial UDFs supported by Sedona can inter-operate seamlessly with other functions supported in sparklyr’s dbplyr SQL translation env. For example, the code below finds the average area of all polygons in polygon_sdf:

The first time you load Sedona, Spark will download all the dependent jars, which can take a few minutes and cause the connection to timeout. You can either retry (some jars will already be downloaded and cached) or increase the `"sparklyr.connect.timeout"` parameter in the sparklyr config.

``` r
library(sparklyr)
library(apache.sedona)

## Only if using development version:
Sys.setenv("SEDONA_JAR_FILES" = "<path to sedona-spark-shaded jar>:<path to sedona-viz jar>")

sc <- spark_connect(master = "local")
polygon_sdf <- spark_read_geojson(sc, location = "/tmp/polygon.json")
```

``` r
mean_area_sdf <- polygon_sdf %>%
  dplyr::summarize(mean_area = mean(ST_Area(geometry)))
print(mean_area_sdf)
```

Notice that all of the above can open up many interesting possibilities. For example, one can extract ML features from geospatial data in Spark dataframes, build a ML pipeline using `ml_*` family of functions in `{sparklyr}` to work with such features, and if the output of a ML model happens to be a geospatial object as well, one can even apply visualization routines in `{apache.sedona}` to visualize the difference between any predicted geometry and the corresponding ground truth.
