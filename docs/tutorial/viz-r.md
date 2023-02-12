# Map Visualization applications in R language


An important part of `apache.sedona` is its collection of R interfaces
to Sedona visualization routines. For example, the following is
essentially the R equivalent of [this example in
Scala](https://github.com/apache/sedona/blob/f6b1c5e24bdb67d2c8d701a9b2af1fb5658fdc4d/viz/src/main/scala/org/apache/sedona/viz/showcase/ScalaExample.scala#L142-L160).

```r
library(sparklyr)
library(apache.sedona)

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

It will create a scatter plot, and then overlay it on top of a
choropleth map, as shown below:

<img src="../../image/choropleth-map.png" width=800 />

See `?apache.sedona::sedona_render_scatter_plot`,
`?apache.sedona::sedona_render_heatmap`, and
`?apache.sedona::sedona_render_choropleth_map` for more details on
visualization-related R interfaces currently implemented by
`apache.sedona`.
