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

import org.apache.sedona.sql.RasterRegistrator.logger

/**
 * A helper object to check if GeoTools GridCoverage2D is available on the classpath.
 */
object GeoToolsCoverageAvailability {
  val gridClassName = "org.geotools.coverage.grid.GridCoverage2D"

  lazy val isGeoToolsAvailable: Boolean = {
    try {
      Class.forName(gridClassName, true, Thread.currentThread().getContextClassLoader)
      true
    } catch {
      case _: ClassNotFoundException =>
        logger.warn(
          "Geotools was not found on the classpath. Raster operations will not be available.")
        false
    }
  }
}
