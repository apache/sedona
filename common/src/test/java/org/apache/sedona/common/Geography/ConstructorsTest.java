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
package org.apache.sedona.common.Geography;

import static org.junit.Assert.*;

import com.google.common.geometry.S2LatLng;
import com.google.common.geometry.S2Point;
import org.apache.sedona.common.S2Geography.Geography;
import org.apache.sedona.common.S2Geography.SinglePointGeography;
import org.apache.sedona.common.S2Geography.WKBWriter;
import org.apache.sedona.common.geography.Constructors;
import org.junit.Test;
import org.locationtech.jts.io.ParseException;

public class ConstructorsTest {

  @Test
  public void geogFromEWKT() throws ParseException {
    assertNull(Constructors.geogFromEWKT(null));

    Geography geog = Constructors.geogFromEWKT("POINT (1 1)");
    assertEquals(0, geog.getSRID());
    assertEquals("POINT (1 1)", geog.toString());

    geog = Constructors.geogFromEWKT("SRID=4269; POINT (1 1)");
    assertEquals(4269, geog.getSRID());
    assertEquals("POINT (1 1)", geog.toString());

    geog = Constructors.geogFromEWKT("SRID=4269;POINT (1 1)");
    assertEquals(4269, geog.getSRID());
    assertEquals("POINT (1 1)", geog.toString());

    ParseException invalid =
        assertThrows(ParseException.class, () -> Constructors.geogFromEWKT("not valid"));
    assertEquals("Unknown geography type: NOT (line 1)", invalid.getMessage());
  }

  @Test
  public void geogFromWKB() throws ParseException {
    S2Point pt = S2LatLng.fromDegrees(45, -64).toPoint();
    SinglePointGeography geog = new SinglePointGeography(pt);

    // Test WKB without SRID
    WKBWriter wkbWriter = new WKBWriter();
    byte[] wkb = wkbWriter.write(geog);

    Geography result = Constructors.geogFromWKB(wkb);
    assertEquals(geog.toString(), result.toString());
    assertEquals(0, result.getSRID());

    // Test specifying SRID
    result = Constructors.geogFromWKB(wkb, 1000);
    assertEquals(geog.toString(), result.toString());
    assertEquals(1000, result.getSRID());

    // Test EWKB with SRID
    wkbWriter = new WKBWriter(2, true);
    geog.setSRID(2000);
    wkb = wkbWriter.write(geog);
    result = Constructors.geogFromWKB(wkb);
    assertEquals(geog.toString(), result.toString());
    assertEquals(2000, result.getSRID());

    // Test overriding SRID
    result = Constructors.geogFromWKB(wkb, 3000);
    assertEquals(geog.toString(), result.toString());
    assertEquals(3000, result.getSRID());
    result = Constructors.geogFromWKB(wkb, 0);
    assertEquals(geog.toString(), result.toString());
    assertEquals(0, result.getSRID());
  }
}
