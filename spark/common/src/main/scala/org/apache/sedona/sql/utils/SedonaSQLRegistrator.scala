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
package org.apache.sedona.sql.utils

import org.apache.sedona.spark.SedonaContext
import org.apache.sedona.sql.RasterRegistrator
import org.apache.sedona.sql.UDF.Catalog
import org.apache.spark.sql.{SQLContext, SparkSession}

@deprecated("Use SedonaContext instead", "1.4.1")
object SedonaSQLRegistrator {
  @deprecated("Use SedonaContext.create instead", "1.4.1")
  def registerAll(sqlContext: SQLContext): Unit = {
    registerAll(sqlContext, "java")
  }

  @deprecated("Use SedonaContext.create instead", "1.4.1")
  def registerAll(sparkSession: SparkSession): Unit =
    registerAll(sparkSession, "java")

  @deprecated("Use SedonaContext.create instead", "1.4.1")
  def registerAll(sqlContext: SQLContext, language: String): Unit = {
    SedonaContext.create(sqlContext.sparkSession, language)
  }

  @deprecated("Use SedonaContext.create instead", "1.4.1")
  def registerAll(sparkSession: SparkSession, language: String): Unit =
    SedonaContext.create(sparkSession, language)

  def dropAll(sparkSession: SparkSession): Unit = {
    Catalog.dropAll(sparkSession)
    RasterRegistrator.dropAll(sparkSession)
  }
}
