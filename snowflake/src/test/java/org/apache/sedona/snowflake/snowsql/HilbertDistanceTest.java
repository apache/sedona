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

import static org.junit.Assert.assertEquals;

import org.apache.sedona.common.Constructors;
import org.junit.Test;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;

public class HilbertDistanceTest {

  @Test
  public void binaryAndNativeGeometryWrappersPreserveUnsignedAddresses() throws ParseException {
    Geometry unitSquare = Constructors.geomFromWKT("POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))", 0);
    Geometry lowerRight = Constructors.geomFromWKT("POINT (1 0)", 0);

    assertEquals(
        2L, UDFs.ST_HilbertDistance(GeometrySerde.serialize(unitSquare), 0.0, 0.0, 1.0, 1.0, 2));
    assertEquals(
        2L, UDFsV2.ST_HilbertDistance(GeometrySerde.serGeoJson(unitSquare), 0.0, 0.0, 1.0, 1.0, 2));

    assertEquals(
        4294967295L,
        UDFs.ST_HilbertDistance(GeometrySerde.serialize(lowerRight), 0.0, 0.0, 1.0, 1.0, 16));
    assertEquals(
        4294967295L,
        UDFsV2.ST_HilbertDistance(GeometrySerde.serGeoJson(lowerRight), 0.0, 0.0, 1.0, 1.0, 16));
  }
}
