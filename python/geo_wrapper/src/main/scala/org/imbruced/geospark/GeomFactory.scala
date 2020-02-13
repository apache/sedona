package org.imbruced.geospark

import com.vividsolutions.jts.geom.{Coordinate, GeometryFactory, Point}

object GeomFactory {
  def createPoint(coordinate: Coordinate): Point = {
    new GeometryFactory().createPoint(coordinate)
  }
}
