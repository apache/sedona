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
package org.apache.sedona.snowflake.snowsql;

import org.apache.sedona.common.FunctionsApacheSIS;
import org.apache.sedona.common.FunctionsGeoTools;
import org.geotools.api.referencing.FactoryException;
import org.geotools.api.referencing.operation.TransformException;
import org.locationtech.jts.geom.Geometry;

public class GeoToolsWrapper {
  public static Geometry transform(
      Geometry geometry, String sourceCRS, String targetCRS, boolean lenient) {
    try {
      return FunctionsGeoTools.transform(geometry, sourceCRS, targetCRS, lenient);
    } catch (FactoryException | TransformException e) {
      throw new RuntimeException(e);
    }
  }

  public static Geometry transform(Geometry geometry, String sourceCRS, String targetCRS) {
    try {
      return FunctionsGeoTools.transform(geometry, sourceCRS, targetCRS);
    } catch (FactoryException | TransformException e) {
      throw new RuntimeException(e);
    }
  }

  public static Geometry transform(
      Geometry geometry,
      String sourceCRS,
      String targetCRS,
      Geometry areaOfInterest,
      boolean lenient) {
    return FunctionsApacheSIS.transform(geometry, sourceCRS, targetCRS, areaOfInterest, lenient);
  }
}
