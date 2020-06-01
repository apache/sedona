package org.datasyslab.geospark.python.adapters

import com.vividsolutions.jts.geom.{Geometry, LineString, Point, Polygon}
import com.vividsolutions.jts.index.SpatialIndex
import org.apache.spark.api.java.{JavaRDD, JavaSparkContext}
import org.datasyslab.geospark.spatialRDD.{LineStringRDD, PointRDD, PolygonRDD, SpatialRDD}


object SpatialObjectLoaderAdapter {
  def loadPointSpatialRDD(sc:JavaSparkContext, path: String): PointRDD = {
    new PointRDD(sc.objectFile[Point](path))
  }

  def loadPolygonSpatialRDD(sc: JavaSparkContext, path: String): PolygonRDD = {
    new PolygonRDD(sc.objectFile[Polygon](path))
  }

  def loadSpatialRDD(sc: JavaSparkContext, path: String): SpatialRDD[Geometry] = {
    val spatialRDD = new SpatialRDD[Geometry]
    spatialRDD.rawSpatialRDD = sc.objectFile[Geometry](path)
    spatialRDD
  }

  def loadLineStringSpatialRDD(sc: JavaSparkContext, path: String): LineStringRDD = {
    new LineStringRDD(sc.objectFile[LineString](path))
  }

  def loadIndexRDD(sc: JavaSparkContext, path: String): JavaRDD[SpatialIndex] = {
    sc.objectFile[SpatialIndex](path)
  }
}
