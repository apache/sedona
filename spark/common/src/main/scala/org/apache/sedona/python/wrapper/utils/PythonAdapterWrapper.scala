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
package org.apache.sedona.python.wrapper.utils

import org.apache.sedona.core.spatialRDD.SpatialRDD
import org.apache.sedona.sql.utils.Adapter
import org.apache.spark.api.java.JavaPairRDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.locationtech.jts.geom.Geometry

import scala.jdk.CollectionConverters._

object PythonAdapterWrapper {
  def toSpatialRdd(df: DataFrame, geometryColumn: String, fieldNames: java.util.List[String]): SpatialRDD[Geometry] = {
    Adapter.toSpatialRdd(df, geometryColumn, fieldNames.asScala.toSeq)
  }

  def toDf[T <: Geometry](spatialRDD: SpatialRDD[T], fieldNames: java.util.ArrayList[String], sparkSession: SparkSession): DataFrame = {
    Adapter.toDf(spatialRDD, fieldNames.asScala.toSeq, sparkSession)
  }

  def toDf(spatialPairRDD: JavaPairRDD[Geometry, Geometry],
           leftFieldnames: java.util.ArrayList[String],
           rightFieldNames: java.util.ArrayList[String],
           sparkSession: SparkSession): DataFrame = {
    Adapter.toDf(spatialPairRDD, leftFieldnames.asScala.toSeq, rightFieldNames.asScala.toSeq, sparkSession)
  }
}
