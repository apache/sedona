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
package org.apache.spark.sql.sedona_sql.UDT

import org.slf4j.{Logger, LoggerFactory}
import org.apache.spark.sql.types.UDTRegistration
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.index.SpatialIndex

object UdtRegistratorWrapper {

  val logger: Logger = LoggerFactory.getLogger(getClass)

  def registerAll(): Unit = {
    UDTRegistration.register(classOf[Geometry].getName, classOf[GeometryUDT].getName)
    UDTRegistration.register(classOf[SpatialIndex].getName, classOf[IndexUDT].getName)
    // Rasters requires geotools which is optional.
    val gridClassName = "org.geotools.coverage.grid.GridCoverage2D"
    try {
      // Trigger an exception if geotools is not found.
      java.lang.Class.forName(gridClassName, true, Thread.currentThread().getContextClassLoader)
      UDTRegistration.register(gridClassName, classOf[RasterUDT].getName)
    } catch {
      case e: ClassNotFoundException => logger.warn("Geotools was not found on the classpath. Raster type will not be registered.")
    }
  }
}
