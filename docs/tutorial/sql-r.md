# Spatial SQL applications in R language


In `apache.sedona` , `sdf_register()`, a S3 generic from `sparklyr`
converting a lower-level object to a Spark dataframe, can be applied to
a `SpatialRDD` objects:

``` r
library(sparklyr)
library(apache.sedona)

sc <- spark_connect(master = "local")
polygon_rdd <- sedona_read_geojson(sc, location = "/tmp/polygon.json")
polygon_sdf <- polygon_rdd %>% sdf_register()

polygon_sdf %>% print(n = 3)
```

    ## # Source: spark<?> [?? x 1]
    ##   geometry
    ##   <list>
    ## 1 <POLYGON ((-87.621765 34.873444, -87.617535 34.873369, -87.6123 34.873337, -8…
    ## 2 <POLYGON ((-85.719017 31.297901, -85.715626 31.305203, -85.714271 31.307096, …
    ## 3 <POLYGON ((-86.000685 34.00537, -85.998837 34.009768, -85.998012 34.010398, -…
    ## # … with more rows

The resulting Spark dataframe object can then be modified using `dplyr`
verbs familiar to many R users. In addition, spatial UDFs supported by
Sedona can inter-operate seamlessly with other functions supported in
`sparklyr`’s dbplyr SQL translation env. For example, the code below
finds the average area of all polygons in `polygon_sdf`:

``` r
mean_area_sdf <- polygon_sdf %>%
  dplyr::summarize(mean_area = mean(ST_Area(geometry)))
print(mean_area_sdf)
```

    ## # Source: spark<?> [?? x 1]
    ##   mean_area
    ##       <dbl>
    ## 1   0.00217

Once spatial objects are imported into Spark dataframes, they can also
be easily integrated with other non-spatial attributes, e.g.,

``` r
modified_polygon_sdf <- polygon_sdf %>%
  dplyr::mutate(type = "polygon")
```


Notice that all of the above can open up many interesting possibilities. For
example, one can extract ML features from geospatial data in Spark
dataframes, build a ML pipeline using `ml_*` family of functions in
`sparklyr` to work with such features, and if the output of a ML model
happens to be a geospatial object as well, one can even apply
visualization routines in `apache.sedona` to visualize the difference
between any predicted geometry and the corresponding ground truth.
