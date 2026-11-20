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
package org.apache.sedona.snowflake.snowsql.udtfs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.sedona.common.Constructors;
import org.apache.sedona.snowflake.snowsql.GeometrySerde;
import org.junit.Test;
import org.locationtech.jts.io.ParseException;

public class ST_IsValidDetailTest {

  private static ST_IsValidDetail.OutputRow process(String wkt, int flag) throws ParseException {
    byte[] geometry = GeometrySerde.serialize(Constructors.geomFromWKT(wkt, 0));
    List<ST_IsValidDetail.OutputRow> rows =
        new ST_IsValidDetail().process(geometry, flag).collect(Collectors.toList());
    assertEquals(1, rows.size());
    return rows.get(0);
  }

  @Test
  public void validatorErrorIsReportedWithoutLocation() throws ParseException {
    // Near Double.MAX_VALUE the JTS validator throws instead of reporting the polygon invalid
    String wkt =
        "POLYGON ((-1.7e308 -10, 1.7e308 -10, 1.7e308 0, 1.7e308 10, -1.7e308 10, -1.7e308 -10),"
            + " (-1e308 0, -5e307 5, -5e307 -5, -1e308 0))";
    for (int flag : new int[] {0, 1}) {
      ST_IsValidDetail.OutputRow row = process(wkt, flag);
      assertFalse(row.valid);
      assertEquals("IllegalArgumentException: Segment vertex does not intersect ring", row.reason);
      assertNull(row.location);
    }
  }

  @Test
  public void validGeometryHasNoReasonOrLocation() throws ParseException {
    ST_IsValidDetail.OutputRow row = process("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))", 0);
    assertTrue(row.valid);
    assertNull(row.reason);
    assertNull(row.location);
  }
}
