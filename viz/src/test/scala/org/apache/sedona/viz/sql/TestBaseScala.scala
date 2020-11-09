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
package org.apache.sedona.viz.sql

import org.apache.log4j.{Level, Logger}
import org.apache.sedona.sql.utils.SedonaSQLRegistrator
import org.apache.sedona.viz.core.Serde.SedonaVizKryoRegistrator
import org.apache.sedona.viz.sql.utils.SedonaVizRegistrator
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSpec}

trait TestBaseScala extends FunSpec with BeforeAndAfterAll{
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("org.datasyslab").setLevel(Level.WARN)

  var spark:SparkSession = _
  val resourceFolder = System.getProperty("user.dir") + "/src/test/resources/"

  val polygonInputLocationWkt = resourceFolder + "county_small.tsv"
  val polygonInputLocation = resourceFolder + "primaryroads-polygon.csv"
  val csvPointInputLocation = resourceFolder + "arealm.csv"

  override def beforeAll(): Unit = {
    spark = SparkSession.builder().config("spark.serializer", classOf[KryoSerializer].getName).
      config("spark.kryo.registrator", classOf[SedonaVizKryoRegistrator].getName).
      master("local[*]").appName("SedonaVizSQL").getOrCreate()
    SedonaSQLRegistrator.registerAll(spark)
    SedonaVizRegistrator.registerAll(spark)
  }

  override def afterAll(): Unit = {
    spark.stop
  }

}
