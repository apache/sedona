package org.imbruced.geo_pyspark

import com.vividsolutions.jts.geom.Geometry
import com.vividsolutions.jts.index.SpatialIndex
import org.apache.spark.api.java.JavaRDD
import org.datasyslab.geospark.spatialRDD.SpatialRDD

object RawJvmIndexRDDSetter {
  def setRawIndexRDD(spatialRDD: SpatialRDD[Geometry], indexRDD: JavaRDD[SpatialIndex]): Boolean = {
    spatialRDD.indexedRawRDD = indexRDD
    true
  }
}
