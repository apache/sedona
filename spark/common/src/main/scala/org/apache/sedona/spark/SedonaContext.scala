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
package org.apache.sedona.spark

import org.apache.sedona.core.serde.SedonaKryoRegistrator
import org.apache.sedona.sql.UDF.UdfRegistrator
import org.apache.sedona.sql.UDT.UdtRegistrator
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.sedona_sql.optimization.SpatialFilterPushDownForGeoParquet
import org.apache.spark.sql.sedona_sql.strategy.join.JoinQueryDetector
import org.apache.spark.sql.{SQLContext, SparkSession}

object SedonaContext {
  def create(sqlContext: SQLContext): SQLContext = {
    create(sqlContext.sparkSession)
    sqlContext
  }

  /**
    * This is the entry point of the entire Sedona system
    * @param sparkSession
    * @return
    */
  def create(sparkSession: SparkSession):SparkSession = {
    if (!sparkSession.experimental.extraStrategies.exists(_.isInstanceOf[JoinQueryDetector])) {
      sparkSession.experimental.extraStrategies ++= Seq(new JoinQueryDetector(sparkSession))
    }
    if (!sparkSession.experimental.extraOptimizations.exists(_.isInstanceOf[SpatialFilterPushDownForGeoParquet])) {
      sparkSession.experimental.extraOptimizations ++= Seq(new SpatialFilterPushDownForGeoParquet(sparkSession))
    }
    UdtRegistrator.registerAll()
    UdfRegistrator.registerAll(sparkSession)
    sparkSession
  }

  /**
    * This method adds the basic Sedona configurations to the SparkSession
    * Usually the user does not need to call this method directly
    * This is only needed when the user needs to manually configure Sedona
    * @return
    */
  def builder(): SparkSession.Builder = {
    SparkSession.builder().config("spark.serializer", classOf[KryoSerializer].getName).
      config("spark.kryo.registrator", classOf[SedonaKryoRegistrator].getName)
  }
}
