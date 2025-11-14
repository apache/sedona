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
package org.apache.sedona.common.S2Geography;

import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.locationtech.jts.geom.PrecisionModel;
import org.locationtech.jts.io.ParseException;

public class WKTReadWriterTest {

  private final WKTReader reader = new WKTReader() {};

  private String writeWithPrecision(Geography geom, PrecisionModel pm) {
    WKTWriter w = new WKTWriter();
    w.setPrecisionModel(pm);
    return w.write(geom);
  }

  @Test
  public void significantDigits_defaultAndSingle() throws ParseException {
    Geography geog = reader.read("POINT (0 3.333333333333334)");
    String wkt = new WKTWriter().write(geog);
    // default ≈16 digits
    assertEquals("POINT (0 3.3333333333333344)", wkt);

    // single‐precision ≈6 digits
    assertEquals(
        "POINT (0 3.333333)",
        writeWithPrecision(geog, new PrecisionModel(PrecisionModel.FLOATING_SINGLE)));
  }

  @Test
  public void lineString_roundTrip() throws ParseException {
    String wkt = "LINESTRING (30 10, 10 30, 40 40)";
    Geography g = reader.read(wkt);
    assertEquals(wkt, writeWithPrecision(g, new PrecisionModel(PrecisionModel.FIXED)));
  }

  @Test
  public void multilineString_roundTrip() throws ParseException {
    String wkt = "MULTILINESTRING ((30 10, 10 30, 40 40), (30 10, 20 30, 40 40))";
    Geography g = reader.read(wkt);
    assertEquals(wkt, writeWithPrecision(g, new PrecisionModel(PrecisionModel.FIXED)));
  }

  @Test
  public void polygon_roundTrip_simpleAndWithHole() throws ParseException {
    String simple = "POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))";
    assertEquals(
        simple, writeWithPrecision(reader.read(simple), new PrecisionModel(PrecisionModel.FIXED)));

    String withHole =
        "POLYGON ((35 10, 45 45, 15 40, 10 20, 35 10), " + "(20 30, 35 35, 30 20, 20 30))";
    String expected =
        "POLYGON ((35 10, 45 45, 15 40, 10 20, 35 10), " + "(30 20, 35 35, 20 30, 30 20))";
    Geography sp = reader.read(withHole);
    assertEquals(
        expected,
        writeWithPrecision(reader.read(withHole), new PrecisionModel(PrecisionModel.FIXED)));
  }

  @Test
  public void multiGeometries_roundTrip() throws ParseException {
    assertEquals(
        "MULTIPOINT ((10 40), (40 30), (20 20), (30 10))",
        writeWithPrecision(
            reader.read("MULTIPOINT ((10 40), (40 30), (20 20), (30 10))"),
            new PrecisionModel(PrecisionModel.FIXED)));
    assertEquals(
        "MULTILINESTRING " + "((10 10, 20 20, 10 40), (40 40, 30 30, 40 20, 30 10))",
        writeWithPrecision(
            reader.read(
                "MULTILINESTRING " + "((10 10, 20 20, 10 40), (40 40, 30 30, 40 20, 30 10))"),
            new PrecisionModel(PrecisionModel.FIXED)));
    assertEquals(
        "MULTIPOLYGON "
            + "(((30 20, 45 40, 10 40, 30 20)), "
            + "((15 5, 40 10, 10 20, 5 10, 15 5)))",
        writeWithPrecision(
            reader.read(
                "MULTIPOLYGON "
                    + "(((30 20, 45 40, 10 40, 30 20)), "
                    + "((15 5, 40 10, 10 20, 5 10, 15 5)))"),
            new PrecisionModel(PrecisionModel.FIXED)));
  }

  @Test
  public void collection_roundTrip() throws ParseException {
    String wkt =
        "GEOMETRYCOLLECTION ("
            + "POINT (40 10), "
            + "LINESTRING (10 10, 20 20, 10 40), "
            + "POLYGON ((40 40, 20 45, 45 30, 40 40))"
            + ")";
    assertEquals(
        wkt, writeWithPrecision(reader.read(wkt), new PrecisionModel(PrecisionModel.FIXED)));
  }

  @Test
  public void empty_roundTrip() throws ParseException {
    String point = "POINT EMPTY";
    assertEquals(
        point, writeWithPrecision(reader.read(point), new PrecisionModel(PrecisionModel.FIXED)));
    String line = "LINESTRING EMPTY";
    assertEquals(
        line, writeWithPrecision(reader.read(line), new PrecisionModel(PrecisionModel.FIXED)));
    String polygon = "POLYGON EMPTY";
    assertEquals(
        polygon,
        writeWithPrecision(reader.read(polygon), new PrecisionModel(PrecisionModel.FIXED)));
    String multipoint = "MULTIPOINT EMPTY";
    assertEquals(
        multipoint,
        writeWithPrecision(reader.read(multipoint), new PrecisionModel(PrecisionModel.FIXED)));
    String multiline = "MULTILINESTRING EMPTY";
    assertEquals(
        multiline,
        writeWithPrecision(reader.read(multiline), new PrecisionModel(PrecisionModel.FIXED)));
    String multipolygon = "MULTIPOLYGON EMPTY";
    assertEquals(
        multipolygon,
        writeWithPrecision(reader.read(multipolygon), new PrecisionModel(PrecisionModel.FIXED)));
    String collection = "GEOMETRYCOLLECTION EMPTY";
    assertEquals(
        collection,
        writeWithPrecision(reader.read(collection), new PrecisionModel(PrecisionModel.FIXED)));
  }
}
