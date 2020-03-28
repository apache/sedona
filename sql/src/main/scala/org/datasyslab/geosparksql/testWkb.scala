package org.datasyslab.geosparksql

import com.vividsolutions.jts.geom.{Coordinate, GeometryFactory}
import com.vividsolutions.jts.io.{WKBReader, WKBWriter}

object testWkb extends App{
  val geomFactory = new GeometryFactory()
  val point = geomFactory.createPoint(new Coordinate(-84.01, 34.01))

  val writer = new WKBWriter()
  writer.write(point).foreach(println)
}
