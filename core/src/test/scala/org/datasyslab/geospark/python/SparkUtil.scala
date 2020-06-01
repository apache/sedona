package org.datasyslab.geospark.python

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.serializer.KryoSerializer
import org.datasyslab.geospark.monitoring.GeoSparkListener
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.scalatest.FunSuite

abstract class SparkUtil extends FunSuite{
  lazy val sc = SparkUtil.sc
}

object SparkUtil {
  private lazy val sc = {
    val conf = new SparkConf().setAppName("scalaTest").setMaster("local[2]")
    conf.set("spark.serializer", classOf[KryoSerializer].getName)
    conf.set("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)

    val sc = new SparkContext(conf)
    sc.addSparkListener(new GeoSparkListener)
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    sc
  }
}