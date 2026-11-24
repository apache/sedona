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

import org.apache.sedona.sql.UDF.SedonaFunctionNames
import org.apache.sedona.viz.sql.utils.SedonaVizRegistrator
import org.apache.spark.sql.SparkSessionExtensions

class SedonaVizExtensions extends (SparkSessionExtensions => Unit) {
  def apply(e: SparkSessionExtensions): Unit = {
    e.injectCheckRule(spark => {
      // As in SedonaSqlExtensions: this registers for every session, so the function name
      // prefix comes from the Spark config of the application that runs the session. The
      // SparkContext is the session's. apply must not ask for one: a builder can call it before
      // the application's SparkContext exists.
      SedonaFunctionNames.useApplicationConf(spark.sparkContext)
      SedonaVizRegistrator.registerAll(spark)
      _ => ()
    })
  }
}
