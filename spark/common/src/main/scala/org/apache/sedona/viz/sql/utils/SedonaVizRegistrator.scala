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
package org.apache.sedona.viz.sql.utils

import org.apache.sedona.viz.sql.UDF.UdfRegistrator
import org.apache.sedona.viz.sql.UDT.UdtRegistrator
import org.apache.spark.sql.{SQLContext, SparkSession}

object SedonaVizRegistrator {
  def registerAll(sqlContext: SQLContext): Unit = {
    UdtRegistrator.registerAll()
    UdfRegistrator.registerAll(sqlContext)
  }

  def registerAll(sparkSession: SparkSession): Unit = {
    UdtRegistrator.registerAll()
    UdfRegistrator.registerAll(sparkSession)
  }

  def dropAll(sparkSession: SparkSession): Unit = {
    UdfRegistrator.dropAll(sparkSession)
  }
}
