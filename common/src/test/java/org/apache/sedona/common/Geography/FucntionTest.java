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

import org.apache.sedona.common.S2Geography.Geography;
import org.apache.sedona.common.geography.Constructors;
import org.apache.sedona.common.geography.Functions;
import org.junit.Test;
import org.locationtech.jts.io.ParseException;

public class FucntionTest {
  @Test
  public void getEnvelope() throws ParseException {
    String wkt = "MULTIPOINT ((-179 0), (179 1), (-180 10))";
    Geography geography = Constructors.geogFromWKT(wkt, 0);
    System.out.println(geography.toString());
    Geography envelope = Functions.getEnvelope(geography);
    System.out.println(envelope.toString());
    //        expect_equal(rect_multipoint$lat_lo, 0)
    //        expect_equal(rect_multipoint$lat_hi, 10)
    //        expect_equal(rect_multipoint$lng_lo, 179)
    //        expect_equal(rect_multipoint$lng_hi, -179)
  }

  @Test
  public void getEnvelopePoint() throws ParseException {
    String wkt = "POINT (-180 10)";
    Geography geography = Constructors.geogFromWKT(wkt, 0);
    System.out.println(geography.toString());
    Geography envelope = Functions.getEnvelope(geography);
    System.out.println(envelope.toString());
  }

  @Test
  public void getEnvelopeContry() throws ParseException {
    String Netherlands =
        "POLYGON ((3.314971 50.80372, 7.092053 50.80372, 7.092053 53.5104, 3.314971 53.5104, 3.314971 50.80372))";
    Geography geography = Constructors.geogFromWKT(Netherlands, 4326);
    Geography envelope = Functions.getEnvelope(geography);
    System.out.println(envelope.toString());

    //    <-------------------- WESTERN HEMISPHERE | EASTERN HEMISPHERE -------------------->
    //
    //            Longitude: ... -179.8°      -180°| 180°      177.3° ...
    //    ----------------------------------+--------------------------------------------
    //            |
    //            Latitude                         |
    //            -16°   +------------------------+ +------------------------+
    //                   |                        | |                        |
    //                   |       POLYGON 2        | |       POLYGON 1        |
    //                   |                        | |                        |
    //            -18.3° +------------------------+ +------------------------+
    //                                             |
    //                                             |
    //                                             ^
    //                                             |
    //                                          Antimeridian
    //                                    (The map's seam at 180°)

    String Fiji =
        "MULTIPOLYGON ( ( (177.285 -18.28799, 180 -18.28799, 180 -16.02088, 177.285 -16.02088, 177.285 -18.28799) ), ( (-180 -18.28799, -179.7933 -18.28799, -179.7933 -16.02088, -180 -16.02088, -180 -18.28799) ) )";
    geography = Constructors.geogFromWKT(Fiji, 4326);
    envelope = Functions.getEnvelope(geography);
    System.out.println(envelope.toString());

    String Antarctica = "POLYGON ((-180 -90, -180 -63.27066, 180 -63.27066, 180 -90, -180 -90))";
    geography = Constructors.geogFromWKT(Antarctica, 4326);
    envelope = Functions.getEnvelope(geography);
    System.out.println(envelope.toString());

    String multiCountry =
        "MULTIPOLYGON (((-180 -90, -180 -63.27066, 180 -63.27066, 180 -90, -180 -90)),((3.314971 50.80372, 7.092053 50.80372, 7.092053 53.5104, 3.314971 53.5104, 3.314971 50.80372)))";
    geography = Constructors.geogFromWKT(multiCountry, 4326);
    envelope = Functions.getEnvelope(geography);
    System.out.println(envelope.toString());
  }
}
