package org.datasyslab.geospark.monitoring

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext, TaskContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite, FunSuiteLike}

class  GeoSparkMetricSuite extends FunSuite with FunSuiteLike with BeforeAndAfterAll {

  implicit lazy val sc = {
    val conf = new SparkConf().setAppName(classOf[GeoSparkMetricSuite].getName).setMaster("local[2]")
    val sc = new SparkContext(conf)
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    sc
  }

  override def afterAll(): Unit = {
    sc.stop
  }

  test("simple count") {

    val data = 1 to 100
    val rdd = sc.parallelize(data, 5)

    val metric = GeoSparkMetrics.createMetric(sc, "count")

    // Count elements in each partition. For partitions with even IDs count each element twice.
    rdd.map { x =>
      if (TaskContext.getPartitionId() % 2 == 0)
        metric.add(2)
      else
        metric.add(1)
      x
    }.collect

    // Verify metric
    val expectedPartitionIds = 0 to 4
    assert(expectedPartitionIds == metric.value.keys.toSeq.sorted)

    for ((partitionId, count) <- metric.value) {
      val expectedCount = if (partitionId % 2 == 0) 40 else 20
      assert(count == expectedCount)
    }
  }
}
