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
package org.apache.sedona.flink;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;
import static org.junit.Assert.assertEquals;

import java.util.Collections;
import java.util.List;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;
import org.apache.sedona.common.S2Geography.Geography;
import org.apache.sedona.common.geography.Constructors;
import org.apache.sedona.flink.expressions.geography.GeographyConstructors;
import org.junit.BeforeClass;
import org.junit.Test;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.WKBWriter;
import org.locationtech.jts.io.WKTReader;

public class GeographyConstructorTest extends TestBase {

  @BeforeClass
  public static void onceExecutedBeforeAll() {
    initialize();
  }

  /** Build a single-row, single-column source table. */
  private Table sourceTable(String colName, Object value, TypeInformation<?> colType) {
    List<Row> data = Collections.singletonList(Row.of(value));
    RowTypeInfo typeInfo =
        new RowTypeInfo(new TypeInformation<?>[] {colType}, new String[] {colName});
    DataStream<Row> ds = env.fromCollection(data).returns(typeInfo);
    return tableEnv.fromDataStream(ds);
  }

  private Geography evalGeog(Table source, String func, String inputCol) {
    Table out = source.select(call(func, $(inputCol)).as("geog"));
    return first(out).getFieldAs("geog");
  }

  @Test
  public void testGeogFromWKT() throws Exception {
    Table src = sourceTable("wkt", "POINT (1 2)", BasicTypeInfo.STRING_TYPE_INFO);
    Geography result =
        evalGeog(src, GeographyConstructors.ST_GeogFromWKT.class.getSimpleName(), "wkt");
    assertEquals(Constructors.geogFromWKT("POINT (1 2)", 0).toEWKT(), result.toEWKT());
  }

  @Test
  public void testGeogFromWKTWithSrid() throws Exception {
    Table src = sourceTable("wkt", "POINT (1 2)", BasicTypeInfo.STRING_TYPE_INFO);
    Table out =
        src.select(
            call(
                    GeographyConstructors.ST_GeogFromWKT.class.getSimpleName(),
                    $("wkt"),
                    org.apache.flink.table.api.Expressions.lit(4326))
                .as("geog"));
    Geography result = first(out).getFieldAs("geog");
    assertEquals(4326, result.getSRID());
    assertEquals(Constructors.geogFromWKT("POINT (1 2)", 4326).toEWKT(), result.toEWKT());
  }

  @Test
  public void testGeogFromText() throws Exception {
    Table src = sourceTable("wkt", "LINESTRING (0 0, 1 1, 2 2)", BasicTypeInfo.STRING_TYPE_INFO);
    Geography result =
        evalGeog(src, GeographyConstructors.ST_GeogFromText.class.getSimpleName(), "wkt");
    assertEquals(
        Constructors.geogFromWKT("LINESTRING (0 0, 1 1, 2 2)", 0).toEWKT(), result.toEWKT());
  }

  @Test
  public void testGeogFromEWKT() throws Exception {
    Table src = sourceTable("ewkt", "SRID=4326;POINT (1 2)", BasicTypeInfo.STRING_TYPE_INFO);
    Geography result =
        evalGeog(src, GeographyConstructors.ST_GeogFromEWKT.class.getSimpleName(), "ewkt");
    assertEquals(4326, result.getSRID());
    assertEquals(Constructors.geogFromEWKT("SRID=4326;POINT (1 2)").toEWKT(), result.toEWKT());
  }

  @Test
  public void testGeogCollFromText() throws Exception {
    String wkt = "GEOMETRYCOLLECTION (POINT (1 2), LINESTRING (0 0, 1 1))";
    Table src = sourceTable("wkt", wkt, BasicTypeInfo.STRING_TYPE_INFO);
    Geography result =
        evalGeog(src, GeographyConstructors.ST_GeogCollFromText.class.getSimpleName(), "wkt");
    assertEquals(Constructors.geogCollFromText(wkt, 0).toEWKT(), result.toEWKT());
  }

  @Test
  public void testGeogFromWKB() throws Exception {
    Geometry point = new WKTReader().read("POINT (1 2)");
    byte[] wkb = new WKBWriter().write(point);
    Table src = sourceTable("wkb", wkb, PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO);
    Geography result =
        evalGeog(src, GeographyConstructors.ST_GeogFromWKB.class.getSimpleName(), "wkb");
    assertEquals(Constructors.geogFromWKB(wkb, 0).toEWKT(), result.toEWKT());
  }

  @Test
  public void testGeogFromEWKB() throws Exception {
    Geometry point = new WKTReader().read("POINT (3 4)");
    point.setSRID(4326);
    // includeSRID=true writes EWKB carrying the SRID flag + value, so this exercises
    // end-to-end SRID extraction (a plain-WKB input would not).
    byte[] ewkb = new WKBWriter(2, true).write(point);
    Table src = sourceTable("wkb", ewkb, PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO);
    Geography result =
        evalGeog(src, GeographyConstructors.ST_GeogFromEWKB.class.getSimpleName(), "wkb");
    assertEquals(4326, result.getSRID());
    assertEquals(Constructors.geogFromWKB(ewkb).toEWKT(), result.toEWKT());
  }

  @Test
  public void testGeogFromGeoHash() throws Exception {
    Table src = sourceTable("hash", "9q9j8ue2v71y5zzy0s4q", BasicTypeInfo.STRING_TYPE_INFO);
    Geography result =
        evalGeog(src, GeographyConstructors.ST_GeogFromGeoHash.class.getSimpleName(), "hash");
    assertEquals(
        Constructors.geogFromGeoHash("9q9j8ue2v71y5zzy0s4q", null).toEWKT(), result.toEWKT());
  }

  @Test
  public void testGeomToGeography() throws Exception {
    Table src = sourceTable("wkt", "POINT (1 2)", BasicTypeInfo.STRING_TYPE_INFO);
    // Build a geometry column, then convert it to geography.
    Table geomTable =
        src.select(
            call(
                    org.apache.sedona.flink.expressions.Constructors.ST_GeomFromWKT.class
                        .getSimpleName(),
                    $("wkt"))
                .as("geom"));
    Table out =
        geomTable.select(
            call(GeographyConstructors.ST_GeomToGeography.class.getSimpleName(), $("geom"))
                .as("geog"));
    Geography result = first(out).getFieldAs("geog");
    Geometry point = new WKTReader().read("POINT (1 2)");
    assertEquals(Constructors.geomToGeography(point).toEWKT(), result.toEWKT());
  }

  @Test
  public void testGeogToGeometry() throws Exception {
    Table src = sourceTable("wkt", "POINT (1 2)", BasicTypeInfo.STRING_TYPE_INFO);
    Table geogTable =
        src.select(
            call(GeographyConstructors.ST_GeogFromWKT.class.getSimpleName(), $("wkt")).as("geog"));
    Table out =
        geogTable.select(
            call(GeographyConstructors.ST_GeogToGeometry.class.getSimpleName(), $("geog"))
                .as("geom"));
    Geometry result = first(out).getFieldAs("geom");
    Geometry expected = Constructors.geogToGeometry(Constructors.geogFromWKT("POINT (1 2)", 0));
    assertEquals(expected.toText(), result.toText());
  }
}
