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
package org.apache.sedona.common.geometrySerde;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.sedona.common.Constructors;
import org.apache.sedona.common.Functions;
import org.junit.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.CoordinateXY;
import org.locationtech.jts.geom.CoordinateXYM;
import org.locationtech.jts.geom.CoordinateXYZM;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.impl.CoordinateArraySequence;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBWriter;
import org.locationtech.jts.io.WKTReader;

public class GeometryDimensionSerdeTest {
  private static final GeometryFactory FACTORY = new GeometryFactory();
  private static final WkbLayout[] WKB_LAYOUTS = {
    new WkbLayout(CoordinateType.XY, 2, 0, 0, 0),
    new WkbLayout(CoordinateType.XYZ, 3, 0, 1000, 0x80000000),
    new WkbLayout(CoordinateType.XYM, 3, 1, 2000, 0x40000000),
    new WkbLayout(CoordinateType.XYZM, 4, 1, 3000, 0xc0000000)
  };

  private static final class WkbLayout {
    final CoordinateType coordinateType;
    final int dimension;
    final int measures;
    final int isoOffset;
    final int ewkbFlags;

    WkbLayout(
        CoordinateType coordinateType, int dimension, int measures, int isoOffset, int ewkbFlags) {
      this.coordinateType = coordinateType;
      this.dimension = dimension;
      this.measures = measures;
      this.isoOffset = isoOffset;
      this.ewkbFlags = ewkbFlags;
    }
  }

  @Test
  public void nestedCollectionsAndEmptyMultipartMembersSurviveFactoryCopies()
      throws ParseException {
    Geometry empty =
        Constructors.geomFromWKB(new WKBWriter(3).write(new WKTReader().read("POINT Z EMPTY")));
    GeometryFactory factory = empty.getFactory();
    Geometry[] parts =
        new Geometry[] {
          factory.createMultiPoint(
              new Point[] {(Point) empty, factory.createPoint(new Coordinate(1, 2, 3))}),
          factory.createMultiLineString(
              new LineString[] {
                (LineString)
                    Constructors.geomFromWKB(
                        new WKBWriter(3).write(new WKTReader().read("LINESTRING Z EMPTY"))),
                factory.createLineString(
                    new Coordinate[] {new Coordinate(1, 2, 3), new Coordinate(4, 5, 6)})
              }),
          factory.createMultiPolygon(
              new Polygon[] {
                (Polygon)
                    Constructors.geomFromWKB(
                        new WKBWriter(3).write(new WKTReader().read("POLYGON Z EMPTY")))
              })
        };
    GeometryCollection nested =
        factory.createGeometryCollection(
            new Geometry[] {
              factory.createGeometryCollection(parts), FACTORY.createPoint(new CoordinateXY(7, 8))
            });
    nested.setUserData("metadata");
    Geometry changed = Functions.setSRID(nested, 4326);
    assertEquals(0, nested.getSRID());
    assertEquals(4326, changed.getSRID());
    assertEquals(4326, changed.getFactory().getSRID());
    assertNull(changed.getUserData());
    Geometry output = roundTrip(changed);
    assertEquals(2, output.getNumGeometries());
    Geometry children = output.getGeometryN(0);
    for (int i = 0; i < parts.length; i++) {
      Geometry part = children.getGeometryN(i);
      assertEquals(parts[i].getNumGeometries(), part.getNumGeometries());
      assertTrue(part.getGeometryN(0).isEmpty());
      assertEquals(CoordinateType.XYZ, coordinateType(GeometrySerializer.serialize(part)));
    }
    assertEquals(
        CoordinateType.XY, coordinateType(GeometrySerializer.serialize(output.getGeometryN(1))));
  }

  @Test
  public void hexWkbReaderPreservesEmptyZAndInputData() throws ParseException {
    byte[] bytes = new WKBWriter(3, true).write(new WKTReader().read("POLYGON Z EMPTY"));
    org.apache.sedona.common.utils.FormatUtils reader =
        new org.apache.sedona.common.utils.FormatUtils(
            org.apache.sedona.common.enums.FileDataSplitter.WKB, true);
    Geometry geometry = reader.readWkb(WKBWriter.toHex(bytes) + "\tmetadata");
    assertEquals("metadata", geometry.getUserData());
    assertEquals(CoordinateType.XYZ, coordinateType(GeometrySerializer.serialize(geometry)));
  }

  @Test
  public void isoAndEwkbLayoutsSurviveCopiesAndWireBuffers() throws ParseException {
    for (boolean iso : new boolean[] {false, true}) {
      for (WkbLayout layout : WKB_LAYOUTS) {
        int dimension = layout.dimension;
        int measures = layout.measures;
        CoordinateType expected = layout.coordinateType;
        for (int primitive = 1; primitive <= 3; primitive++) {
          for (boolean empty : new boolean[] {false, true}) {
            // Populated lines/polygons are covered elsewhere; this also exercises all-NaN Z/M.
            if (!empty && primitive != 1) continue;
            ByteBuffer wkb = ByteBuffer.allocate(64).order(ByteOrder.LITTLE_ENDIAN);
            wkb.put((byte) 1);
            int type =
                iso ? primitive + layout.isoOffset : primitive | layout.ewkbFlags | 0x20000000;
            wkb.putInt(type);
            if (!iso) wkb.putInt(4326);
            if (primitive == 1) {
              wkb.putDouble(empty ? Double.NaN : 1).putDouble(empty ? Double.NaN : 2);
              for (int ordinate = 2; ordinate < dimension; ordinate++) wkb.putDouble(Double.NaN);
            } else {
              wkb.putInt(0);
            }
            Geometry geometry =
                Constructors.geomFromWKB(java.util.Arrays.copyOf(wkb.array(), wkb.position()));
            assertEquals(iso ? 0 : 4326, geometry.getSRID());
            geometry.setUserData("retained");
            assertEquals("retained", geometry.copy().getUserData());
            for (String buffer : new String[] {"bytebuffer", "unsafe"}) {
              Geometry decoded =
                  GeometrySerializer.deserialize(
                      GeometryBufferFactory.wrap(buffer, GeometrySerializer.serialize(geometry)));
              for (Geometry copy :
                  new Geometry[] {
                    geometry.copy(),
                    geometry.reverse(),
                    geometry.getFactory().createGeometry(geometry),
                    decoded,
                    decoded.copy(),
                    decoded.reverse(),
                    Functions.setSRID(decoded, 3857)
                  }) {
                byte[] bytes = GeometrySerializer.serialize(copy);
                assertEquals(expected, coordinateType(bytes));
                Geometry output = GeometrySerializer.deserialize(bytes);
                CoordinateSequence sequence =
                    output instanceof Point
                        ? ((Point) output).getCoordinateSequence()
                        : output instanceof LineString
                            ? ((LineString) output).getCoordinateSequence()
                            : ((Polygon) output).getExteriorRing().getCoordinateSequence();
                assertSequenceLayout(sequence, dimension, measures);
              }
            }
          }
        }
      }
    }
  }

  @Test
  public void declaredFactoryDoesNotPromoteOrdinaryCoordinates() throws ParseException {
    Geometry source =
        Constructors.geomFromWKB(new WKBWriter(3).write(new WKTReader().read("POINT Z EMPTY")));
    Geometry ordinary = source.getFactory().createPoint(new Coordinate(1, 2));
    assertEquals(CoordinateType.XY, coordinateType(GeometrySerializer.serialize(ordinary)));
    assertEquals(
        CoordinateType.XY,
        coordinateType(
            GeometrySerializer.serialize(
                source.getFactory().createGeometry(FACTORY.createPoint(new Coordinate(1, 2))))));
    assertThrows(ParseException.class, () -> Constructors.geomFromWKB(new byte[] {1, 1}));
  }

  @Test
  public void derivedCoordinatesFromBinaryGeometryFactoriesRemainXy() throws ParseException {
    Geometry polygon = new WKTReader().read("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))");
    for (Geometry input :
        new Geometry[] {
          roundTrip(polygon), Constructors.geomFromWKB(new WKBWriter().write(polygon))
        }) {
      GeometryFactory factory = input.getFactory();
      Geometry points = factory.createMultiPointFromCoords(new Coordinate[] {new Coordinate(3, 4)});
      assertEquals(CoordinateType.XY, coordinateType(GeometrySerializer.serialize(points)));
      assertEquals(3, points.getCoordinate().x, 0);
      assertEquals(4, points.getCoordinate().y, 0);
      CoordinateSequence coordinates = factory.getCoordinateSequenceFactory().create(2, 3);
      coordinates.setOrdinate(0, 0, 1);
      coordinates.setOrdinate(0, 1, 2);
      coordinates.setOrdinate(1, 0, 3);
      coordinates.setOrdinate(1, 1, 4);
      assertEquals(
          CoordinateType.XY,
          coordinateType(GeometrySerializer.serialize(factory.createLineString(coordinates))));
      Geometry generated = Functions.generatePoints(input, 3, 100);
      assertEquals(3, generated.getNumGeometries());
      assertTrue(input.covers(generated));
      assertEquals(CoordinateType.XY, coordinateType(GeometrySerializer.serialize(generated)));
    }
  }

  @Test
  public void wkbResultsUseOrdinaryAllocationForEveryGeometryType() throws ParseException {
    for (String wkt :
        new String[] {
          "POINT EMPTY",
          "LINESTRING EMPTY",
          "POLYGON EMPTY",
          "POLYGON ((0 0, 4 0, 0 4, 0 0), (1 1, 2 1, 1 2, 1 1))",
          "MULTIPOINT (EMPTY, (1 2))",
          "MULTILINESTRING (EMPTY, (0 0, 1 1))",
          "MULTIPOLYGON (EMPTY, ((0 0, 4 0, 0 4, 0 0)))",
          "GEOMETRYCOLLECTION (POINT EMPTY, GEOMETRYCOLLECTION (POLYGON EMPTY))"
        }) {
      Geometry geometry =
          Constructors.geomFromWKB(new WKBWriter().write(new WKTReader().read(wkt)));
      geometry.apply(
          (org.locationtech.jts.geom.GeometryComponentFilter)
              component -> {
                Geometry points =
                    component
                        .getFactory()
                        .createMultiPointFromCoords(new Coordinate[] {new Coordinate(3, 4)});
                assertEquals(
                    wkt, CoordinateType.XY, coordinateType(GeometrySerializer.serialize(points)));
              });
    }
  }

  @Test
  public void wkbReaderPreservesMemberSridsAndDefaultSrid() throws ParseException {
    ByteBuffer bytes = ByteBuffer.allocate(9 + 2 * 25).order(ByteOrder.LITTLE_ENDIAN);
    bytes.put((byte) 1).putInt(7).putInt(2);
    bytes.put((byte) 1).putInt(0x20000001).putInt(4326).putDouble(1).putDouble(2);
    bytes.put((byte) 1).putInt(0x20000001).putInt(3857).putDouble(3).putDouble(4);
    Geometry collection = GeometryWkbReader.read(bytes.array(), 27700);
    assertEquals(27700, collection.getSRID());
    assertEquals(4326, collection.getGeometryN(0).getSRID());
    assertEquals(3857, collection.getGeometryN(1).getSRID());
    assertEquals(1, collection.getGeometryN(0).getCoordinate().x, 0);
    assertEquals(4, collection.getGeometryN(1).getCoordinate().y, 0);
  }

  @Test
  public void factoryCopiesAndSetSridUseTheSameUserDataPolicy() throws ParseException {
    Geometry point = new WKTReader().read("POINT (1 2)");
    for (Geometry input : new Geometry[] {point, roundTrip(point)}) {
      input.setUserData("child metadata");
      Geometry collection = input.getFactory().createGeometryCollection(new Geometry[] {input});
      collection.setUserData("parent metadata");
      for (Geometry copy :
          new Geometry[] {
            collection.getFactory().createGeometry(collection), Functions.setSRID(collection, 4326)
          }) {
        assertNull(copy.getUserData());
        assertNull(copy.getGeometryN(0).getUserData());
      }
      assertEquals("parent metadata", collection.getUserData());
      assertEquals("child metadata", input.getUserData());
    }
  }

  @Test
  public void declaredWkbZSurvivesEmptyAndNaNCoordinates() throws ParseException {
    for (String wkt :
        new String[] {
          "POINT Z EMPTY",
          "LINESTRING Z EMPTY",
          "POLYGON Z EMPTY",
          "POINT Z (1 2 NaN)",
          "LINESTRING Z (1 2 NaN, 3 4 NaN)"
        }) {
      byte[] wkb = new WKBWriter(3).write(new WKTReader().read(wkt.replace("NaN", "9")));
      // WKBWriter infers populated Z from values; supply explicit NaN Z ordinates in the bytes.
      if (wkt.contains("NaN")) {
        ByteBuffer ordinates = ByteBuffer.wrap(wkb).order(ByteOrder.BIG_ENDIAN);
        int start = wkt.startsWith("POINT") ? 5 : 9;
        for (int offset = start + 16; offset < wkb.length; offset += 24) {
          ordinates.putDouble(offset, Double.NaN);
        }
      }
      Geometry input = Constructors.geomFromWKB(wkb);
      assertEquals(wkt, CoordinateType.XYZ, coordinateType(GeometrySerializer.serialize(input)));
      for (String buffer : new String[] {"bytebuffer", "unsafe"}) {
        Geometry decoded =
            GeometrySerializer.deserialize(
                GeometryBufferFactory.wrap(buffer, GeometrySerializer.serialize(input)));
        for (Geometry transformed :
            new Geometry[] {
              decoded, decoded.copy(), decoded.reverse(), Functions.setSRID(decoded, 4326)
            }) {
          assertEquals(
              wkt, CoordinateType.XYZ, coordinateType(GeometrySerializer.serialize(transformed)));
        }
      }
    }
  }

  @Test
  public void leadingNaNDoesNotDropLaterOrdinates() {
    LineString xyz =
        FACTORY.createLineString(
            new Coordinate[] {new Coordinate(0, 0, Double.NaN), new Coordinate(1, 1, 3)});
    LineString xym =
        FACTORY.createLineString(
            new Coordinate[] {new CoordinateXYM(0, 0, Double.NaN), new CoordinateXYM(1, 1, 4)});
    LineString xyzm =
        FACTORY.createLineString(
            new Coordinate[] {
              new CoordinateXYZM(0, 0, Double.NaN, Double.NaN), new CoordinateXYZM(1, 1, 3, 4)
            });

    assertLineRoundTrip(xyz, CoordinateType.XYZ, 3, 0, 3, Double.NaN);
    assertLineRoundTrip(xym, CoordinateType.XYM, 3, 1, Double.NaN, 4);
    assertLineRoundTrip(xyzm, CoordinateType.XYZM, 4, 1, 3, 4);
  }

  @Test
  public void dimensionDetectionTraversesMultipartMembers() {
    LineString first =
        FACTORY.createLineString(
            new Coordinate[] {new Coordinate(0, 0, Double.NaN), new Coordinate(1, 1, Double.NaN)});
    LineString second =
        FACTORY.createLineString(
            new Coordinate[] {new Coordinate(2, 2, 7), new Coordinate(3, 3, 8)});
    MultiLineString input = FACTORY.createMultiLineString(new LineString[] {first, second});

    byte[] bytes = GeometrySerializer.serialize(input);
    MultiLineString output = (MultiLineString) GeometrySerializer.deserialize(bytes);

    assertEquals(CoordinateType.XYZ, coordinateType(bytes));
    assertSequenceLayout(((LineString) output.getGeometryN(0)).getCoordinateSequence(), 3, 0);
    assertSequenceLayout(((LineString) output.getGeometryN(1)).getCoordinateSequence(), 3, 0);
    assertEquals(7, output.getGeometryN(1).getCoordinate().getZ(), 0);
  }

  @Test
  public void measureMetadataAndTypedEmptyMembersKeepTheirLayout() {
    Point allNaNMeasure = FACTORY.createPoint(new CoordinateXYM(1, 2, Double.NaN));
    Point allNaNZM = FACTORY.createPoint(new CoordinateXYZM(1, 2, Double.NaN, Double.NaN));
    assertSequenceLayout(((Point) roundTrip(allNaNMeasure)).getCoordinateSequence(), 3, 1);
    assertSequenceLayout(((Point) roundTrip(allNaNZM)).getCoordinateSequence(), 4, 1);

    Point emptyM = FACTORY.createPoint(emptySequence(3, 1));
    Point emptyZM = FACTORY.createPoint(emptySequence(4, 1));
    assertSequenceLayout(((Point) roundTrip(emptyM)).getCoordinateSequence(), 3, 1);
    assertSequenceLayout(((Point) roundTrip(emptyZM)).getCoordinateSequence(), 4, 1);

    Point measuredPoint = FACTORY.createPoint(new CoordinateXYM(1, 2, 8));
    MultiPoint multiPoint = FACTORY.createMultiPoint(new Point[] {emptyM, measuredPoint});
    MultiPoint multiPointOutput = (MultiPoint) roundTrip(multiPoint);
    assertSequenceLayout(((Point) multiPointOutput.getGeometryN(0)).getCoordinateSequence(), 3, 1);
    assertEquals(8, multiPointOutput.getGeometryN(1).getCoordinate().getM(), 0);

    LineString emptyZMLine = FACTORY.createLineString(emptySequence(4, 1));
    LineString xyzmLine =
        FACTORY.createLineString(
            new Coordinate[] {new CoordinateXYZM(0, 0, 1, 2), new CoordinateXYZM(1, 1, 3, 4)});
    MultiLineString multiLine =
        FACTORY.createMultiLineString(new LineString[] {emptyZMLine, xyzmLine});
    MultiLineString multiLineOutput = (MultiLineString) roundTrip(multiLine);
    assertSequenceLayout(
        ((LineString) multiLineOutput.getGeometryN(0)).getCoordinateSequence(), 4, 1);

    Polygon emptyMPolygon = emptyPolygon(3, 1);
    assertSequenceLayout(
        ((Polygon) roundTrip(emptyMPolygon)).getExteriorRing().getCoordinateSequence(), 3, 1);
    Polygon measuredPolygon = measuredPolygon();
    MultiPolygon multiPolygon =
        FACTORY.createMultiPolygon(new Polygon[] {emptyMPolygon, measuredPolygon});
    MultiPolygon multiPolygonOutput = (MultiPolygon) roundTrip(multiPolygon);
    assertSequenceLayout(
        ((Polygon) multiPolygonOutput.getGeometryN(0)).getExteriorRing().getCoordinateSequence(),
        3,
        1);
    assertEquals(9, multiPolygonOutput.getGeometryN(1).getCoordinates()[0].getM(), 0);
  }

  @Test
  public void geometryCollectionPreservesChildLayoutsIndependently() {
    LineString xyz =
        FACTORY.createLineString(
            new Coordinate[] {new Coordinate(0, 0, Double.NaN), new Coordinate(1, 1, 3)});
    Point xym = FACTORY.createPoint(new CoordinateXYM(2, 2, 4));
    Polygon xyzm =
        FACTORY.createPolygon(
            FACTORY.createLinearRing(
                new Coordinate[] {
                  new CoordinateXYZM(0, 0, 1, 5),
                  new CoordinateXYZM(1, 0, 2, 6),
                  new CoordinateXYZM(0, 1, 3, 7),
                  new CoordinateXYZM(0, 0, 1, 5)
                }));
    GeometryCollection input = FACTORY.createGeometryCollection(new Geometry[] {xyz, xym, xyzm});

    GeometryCollection output = (GeometryCollection) roundTrip(input);

    assertSequenceLayout(((LineString) output.getGeometryN(0)).getCoordinateSequence(), 3, 0);
    assertSequenceLayout(((Point) output.getGeometryN(1)).getCoordinateSequence(), 3, 1);
    assertSequenceLayout(
        ((Polygon) output.getGeometryN(2)).getExteriorRing().getCoordinateSequence(), 4, 1);
  }

  @Test
  public void ordinaryJtsXyRemainsXy() throws ParseException {
    LineString input = (LineString) new WKTReader().read("LINESTRING (0 0, 1 1)");
    assertEquals(3, input.getCoordinateSequence().getDimension());

    byte[] bytes = GeometrySerializer.serialize(input);
    LineString output = (LineString) GeometrySerializer.deserialize(bytes);

    assertEquals(CoordinateType.XY, coordinateType(bytes));
    assertSequenceLayout(output.getCoordinateSequence(), 2, 0);
  }

  @Test
  public void mixedDeclaredLayoutsRequireGeometryCollections() throws ParseException {
    for (String[] wkts :
        new String[][] {
          {"POINT (1 2)", "POINT Z EMPTY"},
          {"LINESTRING (0 0, 1 1)", "LINESTRING Z EMPTY"},
          {"POLYGON ((0 0, 4 0, 0 4, 0 0))", "POLYGON Z EMPTY"}
        }) {
      Geometry xy = roundTrip(new WKTReader().read(wkts[0]));
      Geometry z =
          roundTrip(
              Constructors.geomFromWKB(new WKBWriter(3).write(new WKTReader().read(wkts[1]))));
      assertMixedLayoutsRequireCollection(xy, z);
    }
    ByteBuffer point = ByteBuffer.allocate(29).order(ByteOrder.LITTLE_ENDIAN);
    point.put((byte) 1).putInt(1001).putDouble(1).putDouble(2).putDouble(Double.NaN);
    assertMixedLayoutsRequireCollection(
        roundTrip(new WKTReader().read("POINT (3 4)")),
        roundTrip(Constructors.geomFromWKB(point.array())));
  }

  private static void assertMixedLayoutsRequireCollection(Geometry xy, Geometry z) {
    for (Geometry[] members : new Geometry[][] {{xy, z}, {z, xy}}) {
      Geometry multipart = Functions.createMultiGeometry(members);
      IllegalArgumentException error =
          assertThrows(
              IllegalArgumentException.class, () -> GeometrySerializer.serialize(multipart));
      assertTrue(error.getMessage().contains("heterogeneous dimensional layouts"));
      Geometry collection = roundTrip(FACTORY.createGeometryCollection(members));
      assertEquals(2, collection.getNumGeometries());
      for (int i = 0; i < members.length; i++) {
        assertEquals(members[i].isEmpty(), collection.getGeometryN(i).isEmpty());
        assertEquals(
            members[i] == xy ? CoordinateType.XY : CoordinateType.XYZ,
            coordinateType(GeometrySerializer.serialize(collection.getGeometryN(i))));
      }
    }
  }

  @Test
  public void rejectsRecoverablyHeterogeneousMultipartLayouts() {
    LineString xy =
        FACTORY.createLineString(new Coordinate[] {new CoordinateXY(0, 0), new CoordinateXY(1, 1)});
    LineString xyz =
        FACTORY.createLineString(
            new Coordinate[] {new Coordinate(2, 2, 3), new Coordinate(3, 3, 4)});
    MultiLineString mixedZ = FACTORY.createMultiLineString(new LineString[] {xy, xyz});

    LineString xym =
        FACTORY.createLineString(
            new Coordinate[] {new CoordinateXYM(0, 0, 1), new CoordinateXYM(1, 1, 2)});
    MultiLineString mixedM = FACTORY.createMultiLineString(new LineString[] {xym, xyz});

    assertThrows(IllegalArgumentException.class, () -> GeometrySerializer.serialize(mixedZ));
    assertThrows(IllegalArgumentException.class, () -> GeometrySerializer.serialize(mixedM));
  }

  @Test
  public void rejectsHeterogeneousPolygonRingLayouts() {
    LinearRing shell =
        FACTORY.createLinearRing(
            new Coordinate[] {
              new Coordinate(0, 0, 1),
              new Coordinate(10, 0, 1),
              new Coordinate(0, 10, 1),
              new Coordinate(0, 0, 1)
            });
    LinearRing hole =
        FACTORY.createLinearRing(
            new Coordinate[] {
              new CoordinateXY(1, 1),
              new CoordinateXY(2, 1),
              new CoordinateXY(1, 2),
              new CoordinateXY(1, 1)
            });
    Polygon polygon = FACTORY.createPolygon(shell, new LinearRing[] {hole});

    IllegalArgumentException error =
        assertThrows(IllegalArgumentException.class, () -> GeometrySerializer.serialize(polygon));

    assertEquals(
        "GeometrySerializer cannot encode heterogeneous dimensional layouts in one Polygon "
            + "or multipart geometry. Use homogeneous components or a GeometryCollection.",
        error.getMessage());
  }

  private static void assertLineRoundTrip(
      LineString input,
      CoordinateType expectedType,
      int expectedDimension,
      int expectedMeasures,
      double expectedZ,
      double expectedM) {
    byte[] bytes = GeometrySerializer.serialize(input);
    LineString output = (LineString) GeometrySerializer.deserialize(bytes);
    CoordinateSequence sequence = output.getCoordinateSequence();

    assertEquals(expectedType, coordinateType(bytes));
    assertSequenceLayout(sequence, expectedDimension, expectedMeasures);
    assertOrdinate(expectedZ, sequence.getCoordinate(1).getZ());
    assertOrdinate(expectedM, sequence.getCoordinate(1).getM());
    assertTrue(Double.isNaN(sequence.getCoordinate(0).getZ()));
  }

  private static void assertSequenceLayout(
      CoordinateSequence sequence, int expectedDimension, int expectedMeasures) {
    assertEquals(expectedDimension, sequence.getDimension());
    assertEquals(expectedMeasures, sequence.getMeasures());
  }

  private static void assertOrdinate(double expected, double actual) {
    if (Double.isNaN(expected)) {
      assertTrue(Double.isNaN(actual));
    } else {
      assertEquals(expected, actual, 0);
    }
  }

  private static CoordinateType coordinateType(byte[] bytes) {
    return CoordinateType.valueOf(((bytes[0] & 0xFF) & 0x0F) >> 1);
  }

  private static Geometry roundTrip(Geometry geometry) {
    return GeometrySerializer.deserialize(GeometrySerializer.serialize(geometry));
  }

  private static CoordinateSequence emptySequence(int dimension, int measures) {
    return new CoordinateArraySequence(new Coordinate[0], dimension, measures);
  }

  private static Polygon emptyPolygon(int dimension, int measures) {
    return FACTORY.createPolygon(FACTORY.createLinearRing(emptySequence(dimension, measures)));
  }

  private static Polygon measuredPolygon() {
    LinearRing shell =
        FACTORY.createLinearRing(
            new Coordinate[] {
              new CoordinateXYM(0, 0, 9),
              new CoordinateXYM(1, 0, 10),
              new CoordinateXYM(0, 1, 11),
              new CoordinateXYM(0, 0, 9)
            });
    return FACTORY.createPolygon(shell);
  }
}
