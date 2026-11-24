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
package org.apache.sedona.sql.UDF

import org.apache.sedona.viz.sql.UDF.{UdfRegistrator, Catalog => VizCatalog}
import org.apache.spark.sql.sedona_sql.expressions.{ST_3DExtent, ST_Envelope_Aggr, ST_Extent, ST_Intersection_Aggr, ST_Union_Aggr}
import org.apache.spark.sql.sedona_sql.expressions.raster.RS_Stack_Aggr

/**
 * The plain names of every function the registrators in this module register. Each registrator
 * hands all of them to SedonaFunctionNames, so a prefix that turns one of them into another is
 * rejected whichever registrator runs first.
 */
private[sedona] object PlainFunctionNames {
  lazy val all: Seq[String] =
    Catalog.expressions.map(_._1.funcName) ++
      // Read from the classes: building one of these aggregators needs the geometry or raster
      // UDT, and the first registrator can run before either is registered.
      Seq(
        classOf[ST_Envelope_Aggr],
        classOf[ST_Extent],
        classOf[ST_3DExtent],
        classOf[ST_Intersection_Aggr],
        classOf[ST_Union_Aggr],
        classOf[RS_Stack_Aggr]).map(_.getSimpleName) ++
      Catalog.aggregateAliasNames ++
      VizCatalog.expressions.map(UdfRegistrator.plainScalarName) ++
      VizCatalog.aggregateExpressions.map(UdfRegistrator.plainAggregateName)
}
