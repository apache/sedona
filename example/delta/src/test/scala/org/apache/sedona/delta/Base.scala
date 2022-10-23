/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.sedona.delta


import org.apache.sedona.core.serde.SedonaKryoRegistrator
import org.apache.sedona.sql.utils.SedonaSQLRegistrator
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.locationtech.jts.geom.{Coordinate, Geometry, GeometryFactory}
import org.locationtech.jts.io.WKTReader
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSpec, GivenWhenThen}

import java.io.File
import java.nio.file.Paths
import scala.math.random
import scala.reflect.io.Directory


class Base extends FunSpec with BeforeAndAfterAll with GivenWhenThen with BeforeAndAfterEach with Matchers {

  private val warehouseLocation: String = System.getProperty("user.dir") + "/target/"
  protected val geometryFactory = new GeometryFactory()

  val spark: SparkSession = SparkSession
    .builder
    .config("spark.serializer", classOf[KryoSerializer].getName)
    .config("spark.kryo.registrator", classOf[SedonaKryoRegistrator].getName)
    .master("local[*]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.warehouse.dir", warehouseLocation)
    .getOrCreate()

  SedonaSQLRegistrator.registerAll(spark)

  spark.sparkContext.setLogLevel("ERROR")

  protected def createTableAsSelect(df: DataFrame, tableName: String): Unit = {
    df.createOrReplaceTempView("df")

    spark.sql(
      s"CREATE TABLE IF NOT EXISTS $tableName using delta LOCATION '$temporarySavePath/$tableName' AS SELECT * FROM df"
    )
  }
  protected def loadDeltaTable(tableName: String): DataFrame = {
    spark.sql(s"SELECT * FROM delta.`$temporarySavePath/$tableName`")
  }

  protected val temporarySavePath: String = Paths.get(
    System.getProperty("user.dir"), "delta/src/test/scala/tmp").toString

  override def afterEach(): Unit = {
    val tmpDirectory = new Directory(new File(temporarySavePath))
    tmpDirectory.deleteRecursively()
    new File(temporarySavePath).delete()
  }

  protected def createPoint(x: Double, y: Double): Geometry =
    geometryFactory.createPoint(new Coordinate(x, y)).asInstanceOf[Geometry]

  protected def produceDeltaPath(tableName: String): String =
    s"delta.`${temporarySavePath}/${tableName}`"

  protected def produceDeltaTargetPath(tableName: String): String =
    s"${temporarySavePath}/${tableName}"

  protected def createSpatialPointDataFrame(numberOfPoints: Int): DataFrame = {
    import spark.implicits._
    (1 to numberOfPoints).map(index =>
      (index, createPoint(random.floatValue() * 20.0, random.floatValue() * 20.0))
    ).toDF("index", "geom")
  }

  protected val wktReader: WKTReader = new WKTReader()
}
