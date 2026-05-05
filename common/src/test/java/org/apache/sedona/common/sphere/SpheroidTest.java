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
package org.apache.sedona.common.sphere;

import static org.apache.sedona.common.Constructors.geomFromWKT;
import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.io.ParseException;

public class SpheroidTest {

  @Test
  public void testAngularHeight() throws ParseException {
    Double actual =
        Spheroid.angularHeight(geomFromWKT("LINESTRING (0 90, 0 -90)", 4326).getEnvelopeInternal());

    assertEquals(180.0, actual, 0.5);
  }

  @Test
  public void testAngularWidth() throws ParseException {
    Double actual =
        Spheroid.angularWidth(geomFromWKT("LINESTRING (-90 0, 90 0)", 4326).getEnvelopeInternal());

    assertEquals(180.0, actual, 0.5);
  }

  @Test
  public void testAngularWidthAndHeightNearPolesAndAntiMeridian() throws ParseException {
    Envelope env =
        geomFromWKT("LINESTRING (-179.9999994 -82.42408, -157.330902 -85.0511284)", 4326)
            .getEnvelopeInternal();
    Double actualAngularHeight = Spheroid.angularHeight(env);
    Double actualAngularWidth = Spheroid.angularWidth(env);

    assertEquals(2.63, actualAngularHeight, 0.01);
    assertEquals(2.46, actualAngularWidth, 0.01);
  }
}
