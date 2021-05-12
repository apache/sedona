#' Construct a bounding box object.
#'
#' Construct a axis-aligned rectangular bounding box object.
#'
#' @param sc The Spark connection.
#' @param min_x Minimum x-value of the bounding box, can be +/- Inf.
#' @param max_x Maximum x-value of the bounding box, can be +/- Inf.
#' @param min_y Minimum y-value of the bounding box, can be +/- Inf.
#' @param max_y Maximum y-value of the bounding box, can be +/- Inf.
#'
#' @export
new_bounding_box <- function(
                             sc,
                             min_x = -Inf,
                             max_x = Inf,
                             min_y = -Inf,
                             max_y = Inf) {
  invoke_new(
    sc,
    "org.locationtech.jts.geom.Envelope",
    as.numeric(min_x),
    as.numeric(max_x),
    as.numeric(min_y),
    as.numeric(max_y)
  ) %>%
    make_bounding_box()
}

make_bounding_box <- function(jobj) {
  structure(
    list(
      .jobj = jobj,
      minX = function() {
        jobj %>% invoke("getMinX")
      },
      maxX = function() {
        jobj %>% invoke("getMaxX")
      },
      minY = function() {
        jobj %>% invoke("getMinY")
      },
      maxY = function() {
        jobj %>% invoke("getMaxY")
      },
      width = function() {
        jobj %>% invoke("getWidth")
      },
      height = function() {
        jobj %>% invoke("getHeight")
      },
      diameter = function() {
        jobj %>% invoke("getDiameter")
      },
      minExtent = function() {
        jobj %>% invoke("minExtent")
      },
      maxExtent = function() {
        jobj %>% invoke("maxExtent")
      },
      area = function() {
        jobj %>% invoke("getArea")
      }
    ),
    class = "bounding_box"
  )
}
