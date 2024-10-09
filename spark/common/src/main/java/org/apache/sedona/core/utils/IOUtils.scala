package org.apache.sedona.core.utils

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.{Coordinate, Point}

import java.io.File
import scala.io.Source

object IOUtils {
  def createFile(path: String) = {
    val file = new File(path)
    if (!file.getParentFile.exists) file.getParentFile.mkdirs
  }
}
