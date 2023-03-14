#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

#' Visualization routine for Sedona spatial RDD.
#'
#' Generate a visual representation of geometrical object(s) within a Sedona
#' spatial RDD.
#'
#' @param rdd A Sedona spatial RDD.
#' @param resolution_x Resolution on the x-axis.
#' @param resolution_y Resolution on the y-axis.
#' @param output_location Location of the output image. This should be the
#'   desired path of the image file excluding extension in its file name.
#' @param output_format File format of the output image. Currently "png",
#'   "gif", and "svg" formats are supported (default: "png").
#' @param boundary Only render data within the given rectangular boundary.
#'   The `boundary` parameter can be set to either a numeric vector of
#'   c(min_x, max_y, min_y, max_y) values, or with a bounding box object
#'   e.g., new_bounding_box(sc, min_x, max_y, min_y, max_y), or NULL
#'   (the default). If `boundary` is NULL, then the minimum bounding box of the
#'   input spatial RDD will be computed and used as boundary for rendering.
#' @param color_of_variation Which color channel will vary depending on values
#'   of data points. Must be one of "red", "green", or "blue". Default: red.
#' @param base_color Color of any data point with value 0. Must be a numeric
#'   vector of length 3 specifying values for red, green, and blue channels.
#'   Default: c(0, 0, 0).
#' @param shade Whether data point with larger magnitude will be displayed with
#'   darker color. Default: TRUE.
#' @param overlay A `viz_op` object containing a raster image to be
#'   displayed on top of the resulting image.
#' @param browse Whether to open the rendered image in a browser (default:
#'   interactive()).
#'
#' @name sedona_visualization_routines
#' @keywords internal
NULL

#' Visualize a Sedona spatial RDD using a heatmap.
#'
#' Generate a heatmap of geometrical object(s) within a Sedona spatial RDD.
#'
#' @inheritParams sedona_visualization_routines
#' @param blur_radius Controls the radius of a Gaussian blur in the resulting
#'   heatmap.
#'
#' @return No return value.
#'
#' @examples
#'
#' library(sparklyr)
#' library(apache.sedona)
#'
#' sc <- spark_connect(master = "spark://HOST:PORT")
#'
#' if (!inherits(sc, "test_connection")) {
#'   input_location <- "/dev/null" # replace it with the path to your input file
#'   rdd <- sedona_read_dsv_to_typed_rdd(
#'     sc,
#'     location = input_location,
#'     type = "point"
#'   )
#'
#'   sedona_render_heatmap(
#'     rdd,
#'     resolution_x = 800,
#'     resolution_y = 600,
#'     output_location = tempfile("points-"),
#'     output_format = "png",
#'     boundary = c(-91, -84, 30, 35),
#'     blur_radius = 10
#'   )
#' }
#'
#' @family Sedona visualization routines
#'
#' @export
sedona_render_heatmap <- function(rdd,
                                  resolution_x,
                                  resolution_y,
                                  output_location,
                                  output_format = c("png", "gif", "svg"),
                                  boundary = NULL,
                                  blur_radius = 10L,
                                  overlay = NULL,
                                  browse = interactive()) {
  sc <- spark_connection(rdd$.jobj)
  output_format <- match.arg(output_format)

  boundary <- validate_boundary(rdd, boundary)
  viz_op <- invoke_new(
    sc,
    "org.apache.sedona.viz.extension.visualizationEffect.HeatMap",
    as.integer(resolution_x),
    as.integer(resolution_y),
    boundary$.jobj,
    FALSE,
    as.integer(blur_radius)
  )

  rdd %>% gen_raster_image(
    viz_op = viz_op,
    output_location = output_location,
    output_format = output_format,
    overlay = overlay
  )
  if (browse) {
    utils::browseURL(paste0(output_location, ".", tolower(output_format)))
  }

  invisible(viz_op %>% new_viz_op("heatmap"))
}

#' Visualize a Sedona spatial RDD using a scatter plot.
#'
#' Generate a scatter plot of geometrical object(s) within a Sedona spatial RDD.
#'
#' @inheritParams sedona_visualization_routines
#' @param reverse_coords Whether to reverse spatial coordinates in the plot
#'   (default: FALSE).
#'
#' @return No return value.
#'
#' @examples
#' library(sparklyr)
#' library(apache.sedona)
#'
#' sc <- spark_connect(master = "spark://HOST:PORT")
#'
#' if (!inherits(sc, "test_connection")) {
#'   input_location <- "/dev/null" # replace it with the path to your input file
#'   rdd <- sedona_read_dsv_to_typed_rdd(
#'     sc,
#'     location = input_location,
#'     type = "point"
#'   )
#'
#'   sedona_render_scatter_plot(
#'     rdd,
#'     resolution_x = 800,
#'     resolution_y = 600,
#'     output_location = tempfile("points-"),
#'     output_format = "png",
#'     boundary = c(-91, -84, 30, 35)
#'   )
#' }
#'
#' @family Sedona visualization routines
#'
#' @export
sedona_render_scatter_plot <- function(rdd,
                                       resolution_x,
                                       resolution_y,
                                       output_location,
                                       output_format = c("png", "gif", "svg"),
                                       boundary = NULL,
                                       color_of_variation = c("red", "green", "blue"),
                                       base_color = c(0, 0, 0),
                                       shade = TRUE,
                                       reverse_coords = FALSE,
                                       overlay = NULL,
                                       browse = interactive()) {
  viz_op <- sedona_render_viz_effect(
    viz_effect_name = "ScatterPlot",
    rdd = rdd,
    resolution_x = resolution_x,
    resolution_y = resolution_y,
    output_location = output_location,
    output_format = output_format,
    boundary = boundary,
    color_of_variation = color_of_variation,
    base_color = base_color,
    shade = shade,
    reverse_coords = reverse_coords,
    overlay = overlay,
    browse = browse
  )

  invisible(viz_op %>% new_viz_op("scatter_plot"))
}

#' Visualize a Sedona spatial RDD using a choropleth map.
#'
#' Generate a choropleth map of a pair RDD assigning integral values to
#' polygons.
#'
#' @inheritParams sedona_visualization_routines
#' @param pair_rdd A pair RDD with Sedona Polygon objects being keys and
#'   java.lang.Long being values.
#' @param reverse_coords Whether to reverse spatial coordinates in the plot
#'   (default: FALSE).
#'
#' @return No return value.
#'
#' @examples
#' library(sparklyr)
#' library(apache.sedona)
#'
#' sc <- spark_connect(master = "spark://HOST:PORT")
#'
#' if (!inherits(sc, "test_connection")) {
#'   pt_input_location <- "/dev/null" # replace it with the path to your input file
#'   pt_rdd <- sedona_read_dsv_to_typed_rdd(
#'     sc,
#'     location = pt_input_location,
#'     type = "point",
#'     first_spatial_col_index = 1
#'   )
#'   polygon_input_location <- "/dev/null" # replace it with the path to your input file
#'   polygon_rdd <- sedona_read_geojson_to_typed_rdd(
#'     sc,
#'     location = polygon_input_location,
#'     type = "polygon"
#'   )
#'   join_result_rdd <- sedona_spatial_join_count_by_key(
#'     pt_rdd,
#'     polygon_rdd,
#'     join_type = "intersect",
#'     partitioner = "quadtree"
#'   )
#'   sedona_render_choropleth_map(
#'     join_result_rdd,
#'     400,
#'     200,
#'     output_location = tempfile("choropleth-map-"),
#'     boundary = c(-86.8, -86.6, 33.4, 33.6),
#'     base_color = c(255, 255, 255)
#'   )
#' }
#'
#' @family Sedona visualization routines
#'
#' @export
sedona_render_choropleth_map <- function(pair_rdd,
                                         resolution_x,
                                         resolution_y,
                                         output_location,
                                         output_format = c("png", "gif", "svg"),
                                         boundary = NULL,
                                         color_of_variation = c("red", "green", "blue"),
                                         base_color = c(0, 0, 0),
                                         shade = TRUE,
                                         reverse_coords = FALSE,
                                         overlay = NULL,
                                         browse = interactive()) {
  viz_op <- sedona_render_viz_effect(
    viz_effect_name = "ChoroplethMap",
    rdd = pair_rdd,
    resolution_x = resolution_x,
    resolution_y = resolution_y,
    output_location = output_location,
    output_format = output_format,
    boundary = boundary,
    color_of_variation = color_of_variation,
    base_color = base_color,
    shade = shade,
    reverse_coords = reverse_coords,
    overlay = overlay,
    browse = browse
  )

  invisible(viz_op %>% new_viz_op("choropleth_map"))
}

sedona_render_viz_effect <- function(viz_effect_name,
                                     rdd,
                                     resolution_x,
                                     resolution_y,
                                     output_location,
                                     output_format = c("png", "gif", "svg"),
                                     boundary = NULL,
                                     color_of_variation = c("red", "green", "blue"),
                                     base_color = c(0, 0, 0),
                                     shade = shade,
                                     reverse_coords = FALSE,
                                     overlay = NULL,
                                     browse = interactive()) {
  sc <- spark_connection(rdd$.jobj)
  output_format <- match.arg(output_format)
  color_of_variation <- match.arg(color_of_variation)
  validate_base_color(base_color)

  boundary <- validate_boundary(rdd, boundary)
  viz_op <- invoke_new(
    sc,
    paste0("org.apache.sedona.viz.extension.visualizationEffect.", viz_effect_name),
    as.integer(resolution_x),
    as.integer(resolution_y),
    boundary$.jobj,
    reverse_coords
  )

  rdd %>%
    gen_raster_image(
      viz_op = viz_op,
      output_location = output_location,
      output_format = output_format,
      color_settings = list(
        color_of_variation = color_of_variation,
        base_color = base_color,
        shade = shade
      ),
      overlay = overlay
    )
  if (browse) {
    utils::browseURL(paste0(output_location, ".", tolower(output_format)))
  }

  invisible(viz_op)
}

validate_base_color <- function(base_color) {
  if (!is.numeric(base_color) || length(base_color) != 3) {
    stop(
      "Base color (`base_color`) must be a numeric vector of length 3 ",
      "specifying values for red, green, and blue channels ",
      "(e.g., c(0, 0, 0))."
    )
  }
}

validate_boundary <- function(rdd, boundary) {
  sc <- spark_connection(rdd$.jobj)

  if (is.null(boundary)) {
    minimum_bounding_box(rdd)
  } else if (inherits(boundary, "bounding_box")) {
    boundary
  } else if (is.numeric(boundary)) {
    if (length(boundary) != 4) {
      stop(
        "Boundary specification with numeric vector must consist of ",
        "exactly 4 values: c(min_x, max_x, min_y, max_y)."
      )
    }
    do.call(new_bounding_box, append(list(sc), as.list(boundary)))
  } else {
    stop(
      "Boundary specification must be either NULL, a numeric vector of ",
      "c(min_x, max_x, min_y, max_y) values, or a bounding box object"
    )
  }
}

gen_raster_image <- function(rdd,
                             viz_op,
                             output_location,
                             output_format,
                             color_settings = NULL,
                             overlay = NULL) {
  sc <- spark_connection(rdd$.jobj)

  image_generator <- invoke_new(
    sc,
    "org.apache.sedona.viz.core.ImageGenerator"
  )
  if (!is.null(color_settings)) {
    customize_color_params <- list(viz_op, "CustomizeColor") %>%
      append(as.list(as.integer(unlist(color_settings$base_color)))) %>%
      append(
        list(
          255L, # gamma
          sc$state$enums$awt_color[[color_settings$color_of_variation]],
          color_settings$shade
        )
      )
    do.call(invoke, customize_color_params)
  }
  invoke(viz_op, "Visualize", java_context(sc), rdd$.jobj)

  invoke(
    image_generator,
    "SaveRasterImageAsLocalFile",
    viz_op %>% process_overlay(overlay),
    output_location,
    sc$state$enums$image_types[[output_format]]
  )
}

process_overlay <- function(viz_op, overlay) {
  if (!is.null(overlay)) {
    sc <- spark_connection(viz_op)

    viz_op_img <- invoke(viz_op, "rasterImage")
    overlay_op <- invoke_new(
      sc,
      "org.apache.sedona.viz.core.RasterOverlayOperator",
      viz_op_img
    )
    overlay_img <- invoke(overlay$.jobj, "rasterImage")
    invoke(overlay_op, "JoinImage", overlay_img)
    invoke(overlay_op, "backRasterImage")
  } else {
    invoke(viz_op, "rasterImage")
  }
}

new_viz_op <- function(jobj, type = NULL) {
  structure(list(.jobj = jobj, class = c(type, "viz_op")))
}
