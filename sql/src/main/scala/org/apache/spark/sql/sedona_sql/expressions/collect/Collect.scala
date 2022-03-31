package org.apache.spark.sql.sedona_sql.expressions.collect

import org.apache.sedona.core.geometryObjects.Circle
import org.locationtech.jts.geom.{Geometry, GeometryCollection, GeometryFactory, LineString, Point, Polygon}
import scala.collection.JavaConverters._


object Collect {
  private val geomFactory = new GeometryFactory()

  def createMultiGeometry(geometries : Seq[Geometry]): Geometry = {
    if (geometries.length>1){
      geomFactory.buildGeometry(geometries.asJava)
    }
    else if(geometries.length==1){
      createMultiGeometryFromOneElement(geometries.head)
    }
    else{
      geomFactory.createGeometryCollection()
    }
  }

  private def createMultiGeometryFromOneElement(geom: Geometry): Geometry = {
    geom match {
      case circle: Circle                 => geomFactory.createGeometryCollection(Array(circle))
      case collection: GeometryCollection => collection
      case string: LineString =>
        geomFactory.createMultiLineString(Array(string))
      case point: Point     => geomFactory.createMultiPoint(Array(point))
      case polygon: Polygon => geomFactory.createMultiPolygon(Array(polygon))
      case _                => geomFactory.createGeometryCollection()
    }
  }
}
