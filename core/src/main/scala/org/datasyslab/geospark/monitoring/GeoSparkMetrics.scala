package org.datasyslab.geospark.monitoring

import org.apache.spark.SparkContext

object GeoSparkMetrics {
  def createMetric(sc: SparkContext, name: String): GeoSparkMetric = {
    val acc = new GeoSparkMetric()
    sc.register(acc, "geospark.spatialjoin." + name)
    acc
  }
}
