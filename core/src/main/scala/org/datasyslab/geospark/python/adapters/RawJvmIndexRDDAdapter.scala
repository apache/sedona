package org.datasyslab.geospark.python.adapters

import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.index.SpatialIndex
import org.apache.spark.api.java.JavaRDD
import org.datasyslab.geospark.spatialRDD.SpatialRDD


object RawJvmIndexRDDAdapter {
  def setRawIndexRDD(spatialRDD: SpatialRDD[Geometry], indexRDD: JavaRDD[SpatialIndex]): Boolean = {
    spatialRDD.indexedRawRDD = indexRDD
    true
  }
}
