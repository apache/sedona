package org.datasyslab.geosparksql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geospark.utils.GeoSparkConf
import org.datasyslab.geosparksql.utils.GeoSparkSQLRegistrator
import org.scalatest.{BeforeAndAfterAll, FunSpec}

trait SparkTestSpec extends FunSpec with BeforeAndAfterAll {
  val appName: String = "testApp"
  val sparkOptions: Map[String, String] = Map()
  var sparkSession: SparkSession = _

  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)

  override def beforeAll(): Unit = {
    var sessionBuidler = SparkSession.builder()
      .config("spark.serializer", classOf[KryoSerializer].getName)
      .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
      .master("local[*]")
      .appName(appName)

    sparkOptions.foreach { case (key, value) =>
      sessionBuidler = sessionBuidler.config(key, value)
    }
    sparkSession = sessionBuidler.getOrCreate()

    val geosparkConf = new GeoSparkConf(sparkSession.sparkContext.getConf)
    println(geosparkConf)

    GeoSparkSQLRegistrator.registerAll(sparkSession.sqlContext)
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    GeoSparkSQLRegistrator.dropAll(sparkSession)
    sparkSession.stop
    super.afterAll()
  }
}