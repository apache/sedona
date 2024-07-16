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
package org.apache.sedona.common.utils;

import static org.locationtech.jts.geom.Coordinate.NULL_ORDINATE;

import java.nio.ByteOrder;
import java.util.*;
import org.apache.sedona.common.Functions;
import org.locationtech.jts.algorithm.Angle;
import org.locationtech.jts.algorithm.distance.DiscreteFrechetDistance;
import org.locationtech.jts.algorithm.distance.DiscreteHausdorffDistance;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.io.ByteOrderValues;
import org.locationtech.jts.io.WKBWriter;
import org.locationtech.jts.io.WKTWriter;
import org.locationtech.jts.operation.polygonize.Polygonizer;
import org.locationtech.jts.operation.union.UnaryUnionOp;
import org.locationtech.spatial4j.context.jts.JtsSpatialContext;
import org.locationtech.spatial4j.shape.jts.JtsGeometry;

public class GeomUtils {
  public static String printGeom(Geometry geom) {
    if (geom.getUserData() != null) return geom.toText() + "\t" + geom.getUserData();
    else return geom.toText();
  }

  public static String printGeom(Object geom) {
    Geometry g = (Geometry) geom;
    return printGeom(g);
  }

  public static int hashCode(Geometry geom) {
    return geom.getUserData() == null
        ? geom.hashCode()
        : geom.hashCode() * 31 + geom.getUserData().hashCode();
  }

  public static boolean equalsTopoGeom(Geometry geom1, Geometry geom2) {
    if (Objects.equals(geom1.getUserData(), geom2.getUserData())) return geom1.equals(geom2);
    return false;
  }

  public static boolean equalsExactGeom(Geometry geom1, Object geom2) {
    if (!(geom2 instanceof Geometry)) return false;
    Geometry g = (Geometry) geom2;
    if (Objects.equals(geom1.getUserData(), g.getUserData())) return geom1.equalsExact(g);
    else return false;
  }

  /**
   * This is for verifying the correctness of two geometries loaded from geojson
   *
   * @param geom1
   * @param geom2
   * @return
   */
  public static boolean equalsExactGeomUnsortedUserData(Geometry geom1, Object geom2) {
    if (!(geom2 instanceof Geometry)) return false;
    Geometry g = (Geometry) geom2;
    if (equalsUserData(geom1.getUserData(), g.getUserData())) return geom1.equalsExact(g);
    else return false;
  }

  /**
   * Use for check if two user data attributes are equal This is mainly used for GeoJSON parser as
   * the column order is uncertain each time
   *
   * @param userData1
   * @param userData2
   * @return
   */
  public static boolean equalsUserData(Object userData1, Object userData2) {
    String[] split1 = ((String) userData1).split("\t");
    String[] split2 = ((String) userData2).split("\t");
    Arrays.sort(split1);
    Arrays.sort(split2);
    return Arrays.equals(split1, split2);
  }

  /** Swaps the XY coordinates of a geometry. */
  public static void flipCoordinates(Geometry g) {
    g.apply(
        new CoordinateSequenceFilter() {

          @Override
          public void filter(CoordinateSequence seq, int i) {
            double oldX = seq.getCoordinate(i).x;
            double oldY = seq.getCoordinateCopy(i).y;
            seq.getCoordinate(i).setX(oldY);
            seq.getCoordinate(i).setY(oldX);
          }

          @Override
          public boolean isGeometryChanged() {
            return true;
          }

          @Override
          public boolean isDone() {
            return false;
          }
        });
  }
  /*
   * Returns a POINT that is guaranteed to lie on the surface.
   */
  public static Geometry getInteriorPoint(Geometry geometry) {
    if (geometry == null) {
      return null;
    }
    return geometry.getInteriorPoint();
  }

  /**
   * Return the nth point from the given geometry (which could be a linestring or a circular
   * linestring) If the value of n is negative, return a point backwards E.g. if n = 1, return 1st
   * point, if n = -1, return last point
   *
   * @param lineString from which the nth point is to be returned
   * @param n is the position of the point in the geometry
   * @return a point
   */
  public static Geometry getNthPoint(LineString lineString, int n) {
    if (lineString == null || n == 0) {
      return null;
    }

    int p = lineString.getNumPoints();
    if (Math.abs(n) > p) {
      return null;
    }

    Coordinate coordinate;
    if (n > 0) {
      coordinate = lineString.getCoordinates()[n - 1];
    } else {
      coordinate = lineString.getCoordinates()[p + n];
    }
    return lineString.getFactory().createPoint(coordinate);
  }

  public static Geometry getExteriorRing(Geometry geometry) {
    try {
      Polygon polygon = (Polygon) geometry;
      return polygon.getExteriorRing();
    } catch (ClassCastException e) {
      return null;
    }
  }

  public static String getEWKT(Geometry geometry) {
    if (geometry == null) {
      return null;
    }

    int srid = geometry.getSRID();
    String sridString = "";
    if (srid != 0) {
      sridString = "SRID=" + String.valueOf(srid) + ";";
    }
    return sridString + new WKTWriter(4).write(geometry);
  }

  public static String getWKT(Geometry geometry) {
    if (geometry == null) {
      return null;
    }
    return new WKTWriter(4).write(geometry);
  }

  public static String getHexEWKB(Geometry geometry, int endian) {
    WKBWriter writer =
        new WKBWriter(GeomUtils.getDimension(geometry), endian, geometry.getSRID() != 0);
    return WKBWriter.toHex(writer.write(geometry));
  }

  public static byte[] getEWKB(Geometry geometry) {
    if (geometry == null) {
      return null;
    }
    int endian =
        ByteOrder.nativeOrder() == ByteOrder.BIG_ENDIAN
            ? ByteOrderValues.BIG_ENDIAN
            : ByteOrderValues.LITTLE_ENDIAN;
    WKBWriter writer =
        new WKBWriter(GeomUtils.getDimension(geometry), endian, geometry.getSRID() != 0);
    return writer.write(geometry);
  }

  public static byte[] getWKB(Geometry geometry) {
    if (geometry == null) {
      return null;
    }
    int endian =
        ByteOrder.nativeOrder() == ByteOrder.BIG_ENDIAN
            ? ByteOrderValues.BIG_ENDIAN
            : ByteOrderValues.LITTLE_ENDIAN;
    WKBWriter writer = new WKBWriter(GeomUtils.getDimension(geometry), endian, false);
    return writer.write(geometry);
  }

  public static Geometry get2dGeom(Geometry geom) {
    Coordinate[] coordinates = geom.getCoordinates();
    GeometryFactory geometryFactory = geom.getFactory();
    CoordinateSequence sequence =
        geometryFactory.getCoordinateSequenceFactory().create(coordinates);
    if (sequence.getDimension() > 2) {
      for (int i = 0; i < coordinates.length; i++) {
        sequence.setOrdinate(i, 2, NULL_ORDINATE);
      }
      if (sequence.getDimension() == 4) {
        for (int i = 0; i < coordinates.length; i++) {
          sequence.setOrdinate(i, 3, NULL_ORDINATE);
        }
      }
    }
    geom.geometryChanged();
    return geom;
  }

  public static Geometry buildArea(Geometry geom) {
    if (geom == null || geom.isEmpty()) {
      return geom;
    }
    Polygonizer polygonizer = new Polygonizer();
    polygonizer.add(geom);
    List<Polygon> polygons = (List<Polygon>) polygonizer.getPolygons();
    if (polygons.isEmpty()) {
      return null;
    } else if (polygons.size() == 1) {
      return polygons.get(0);
    }
    int srid = geom.getSRID();
    Map<Polygon, Polygon> parentMap = findFaceHoles(polygons);
    List<Polygon> facesWithEvenAncestors = new ArrayList<>();
    for (Polygon face : polygons) {
      face.normalize();
      if (countParents(parentMap, face) % 2 == 0) {
        facesWithEvenAncestors.add(face);
      }
    }
    UnaryUnionOp unaryUnionOp = new UnaryUnionOp(facesWithEvenAncestors);
    Geometry outputGeom = unaryUnionOp.union();
    if (outputGeom != null) {
      outputGeom.normalize();
      if (outputGeom.getSRID() != srid) {
        outputGeom = Functions.setSRID(outputGeom, srid);
      }
    }
    return outputGeom;
  }

  public static int getDimension(Geometry geometry) {
    return geometry.getCoordinate() != null
            && !java.lang.Double.isNaN(geometry.getCoordinate().getZ())
        ? 3
        : 2;
  }

  /**
   * Checks if the geometry only contains geometry of the same dimension. By dimension this refers
   * to whether the geometries are all, for example, lines (1D).
   *
   * @param geometry geometry to check
   * @return true iff geometry is homogeneous
   */
  public static boolean geometryIsHomogeneous(Geometry geometry) {
    int dimension = geometry.getDimension();

    if (!geometry.isEmpty()) {
      for (int i = 0; i < geometry.getNumGeometries(); i++) {
        if (dimension != geometry.getGeometryN(i).getDimension()) {
          return false;
        }
      }
    }

    return true;
  }

  /**
   * Checks if either the geometry is, or contains, only point geometry. GeometryCollections that
   * only contain points will return true.
   *
   * @param geometry geometry to check
   * @return true iff geometry is puntal
   */
  public static boolean geometryIsPuntal(Geometry geometry) {
    if (geometry instanceof Puntal) {
      return true;
    } else if (geometryIsHomogeneous(geometry) && geometry.getDimension() == 0) {
      return true;
    }
    return false;
  }

  /**
   * Checks if either the geometry is, or contains, only line geometry. GeometryCollections that
   * only contain lines will return true.
   *
   * @param geometry geometry to check
   * @return true iff geometry is lineal
   */
  public static boolean geometryIsLineal(Geometry geometry) {
    if (geometry instanceof Lineal) {
      return true;
    } else if (geometryIsHomogeneous(geometry) && geometry.getDimension() == 1) {
      return true;
    }
    return false;
  }

  /**
   * Checks if either the geometry is, or contains, only polygon geometry. GeometryCollections that
   * only contain polygons will return true.
   *
   * @param geometry geometry to check
   * @return true iff geometry is polygonal
   */
  public static boolean geometryIsPolygonal(Geometry geometry) {
    if (geometry instanceof Polygonal) {
      return true;
    } else if (geometryIsHomogeneous(geometry) && geometry.getDimension() == 2) {
      return true;
    }
    return false;
  }

  /**
   * Checks if the geoemetry pair - <code>left</code> and <code>right</code> - should be handled be
   * the current partition - <code>extent</code>.
   *
   * @param left
   * @param right
   * @param extent
   * @return
   */
  public static boolean isDuplicate(Geometry left, Geometry right, HalfOpenRectangle extent) {
    // Handle easy case: points. Since each point is assigned to exactly one partition,
    // different partitions cannot emit duplicate results.
    if (left instanceof Point || right instanceof Point) {
      return false;
    }

    // Neither geometry is a point

    // Check if reference point of the intersection of the bounding boxes lies within
    // the extent of this partition. If not, don't run any checks. Let the partition
    // that contains the reference point do all the work.
    Envelope intersection = left.getEnvelopeInternal().intersection(right.getEnvelopeInternal());
    if (!intersection.isNull()) {
      final Point referencePoint =
          left.getFactory()
              .createPoint(new Coordinate(intersection.getMinX(), intersection.getMinY()));
      if (!extent.contains(referencePoint)) {
        return true;
      }
    }

    return false;
  }

  private static Map<Polygon, Polygon> findFaceHoles(List<Polygon> faces) {
    Map<Polygon, Polygon> parentMap = new HashMap<>();
    faces.sort(Comparator.comparing((Polygon p) -> p.getEnvelope().getArea()).reversed());
    for (int i = 0; i < faces.size(); i++) {
      Polygon face = faces.get(i);
      int nHoles = face.getNumInteriorRing();
      for (int h = 0; h < nHoles; h++) {
        Geometry hole = face.getInteriorRingN(h);
        for (int j = i + 1; j < faces.size(); j++) {
          Polygon face2 = faces.get(j);
          if (parentMap.containsKey(face2)) {
            continue;
          }
          Geometry face2ExteriorRing = face2.getExteriorRing();
          if (face2ExteriorRing.equals(hole)) {
            parentMap.put(face2, face);
          }
        }
      }
    }
    return parentMap;
  }

  private static int countParents(Map<Polygon, Polygon> parentMap, Polygon face) {
    int pCount = 0;
    while (parentMap.containsKey(face)) {
      pCount++;
      face = parentMap.get(face);
    }
    return pCount;
  }

  public static <T extends Geometry> List<Geometry> extractGeometryCollection(
      Geometry geom, Class<T> geomType) {
    ArrayList<Geometry> leafs = new ArrayList<>();
    if (!(geom instanceof GeometryCollection)) {
      if (geomType.isAssignableFrom(geom.getClass())) {
        leafs.add(geom);
      }
      return leafs;
    }
    LinkedList<GeometryCollection> parents = new LinkedList<>();
    parents.add((GeometryCollection) geom);
    while (!parents.isEmpty()) {
      GeometryCollection parent = parents.removeFirst();
      for (int i = 0; i < parent.getNumGeometries(); i++) {
        Geometry child = parent.getGeometryN(i);
        if (child instanceof GeometryCollection) {
          parents.add((GeometryCollection) child);
        } else {
          if (geomType.isAssignableFrom(child.getClass())) {
            leafs.add(child);
          }
        }
      }
    }
    return leafs;
  }

  public static List<Geometry> extractGeometryCollection(Geometry geom) {
    return extractGeometryCollection(geom, Geometry.class);
  }

  public static Geometry[] getSubGeometries(Geometry geom) {
    Geometry[] geometries = new Geometry[geom.getNumGeometries()];
    for (int i = 0; i < geom.getNumGeometries(); i++) {
      geometries[i] = geom.getGeometryN(i);
    }
    return geometries;
  }

  public static int getPolygonNumRings(Polygon polygon) {
    LinearRing shell = polygon.getExteriorRing();
    if (shell == null || shell.isEmpty()) {
      return 0;
    } else {
      return 1 + polygon.getNumInteriorRing();
    }
  }

  public static void translateGeom(Geometry geometry, double deltaX, double deltaY, double deltaZ) {
    Coordinate[] coordinates = geometry.getCoordinates();
    for (int i = 0; i < coordinates.length; i++) {
      Coordinate currCoordinate = coordinates[i];
      currCoordinate.setX(currCoordinate.getX() + deltaX);
      currCoordinate.setY(currCoordinate.getY() + deltaY);
      if (!Double.isNaN(currCoordinate.z)) {
        currCoordinate.setZ(currCoordinate.getZ() + deltaZ);
      }
    }
    if (deltaX != 0 || deltaY != 0 || deltaZ != 0) {
      geometry.geometryChanged();
    }
  }

  public static boolean isAnyGeomEmpty(Geometry... geometries) {
    for (Geometry geometry : geometries) {
      if (geometry != null) if (geometry.isEmpty()) return true;
    }
    return false;
  }

  public static Coordinate[] getStartEndCoordinates(Geometry line) {
    if (line.getNumPoints() < 2) return null;
    Coordinate[] coordinates = line.getCoordinates();
    return new Coordinate[] {coordinates[0], coordinates[coordinates.length - 1]};
  }

  public static double calcAngle(
      Coordinate start1, Coordinate end1, Coordinate start2, Coordinate end2) {
    double angle1 = normalizeAngle(Angle.angle(start1, end1));
    double angle2 = normalizeAngle(Angle.angle(start2, end2));
    return normalizeAngle(angle1 - angle2);
  }

  private static double normalizeAngle(double angle) {
    if (angle < 0) {
      return 2 * Math.PI - Math.abs(angle);
    }
    return angle;
  }

  public static double toDegrees(double angleInRadian) {
    return Angle.toDegrees(angleInRadian);
  }

  public static void affineGeom(
      Geometry geometry,
      Double a,
      Double b,
      Double c,
      Double d,
      Double e,
      Double f,
      Double g,
      Double h,
      Double i,
      Double xOff,
      Double yOff,
      Double zOff) {

    Coordinate[] coordinates = geometry.getCoordinates();
    for (Coordinate currCoordinate : coordinates) {
      double x = currCoordinate.getX(),
          y = currCoordinate.getY(),
          z = Double.isNaN(currCoordinate.getZ()) ? 0 : currCoordinate.getZ();
      double newX = a * x + b * y + xOff;
      if (c != null) newX += c * z;
      double newY = d * x + e * y + yOff;
      if (f != null) newY += f * z;
      currCoordinate.setX(newX);
      currCoordinate.setY(newY);

      if (g != null && h != null && i != null && !Double.isNaN(currCoordinate.getZ())) {
        double newZ = g * x + h * y + i * z + zOff;
        currCoordinate.setZ(newZ);
      }
    }
    geometry.geometryChanged();
  }

  public static double getFrechetDistance(Geometry g1, Geometry g2) {
    if (g1.isEmpty() || g2.isEmpty()) return 0.0;
    return DiscreteFrechetDistance.distance(g1, g2);
  }

  public static Double getHausdorffDistance(Geometry g1, Geometry g2, double densityFrac) {
    if (g1.isEmpty() || g2.isEmpty()) return 0.0;
    DiscreteHausdorffDistance hausdorffDistanceObj = new DiscreteHausdorffDistance(g1, g2);
    if (densityFrac != -1) {
      hausdorffDistanceObj.setDensifyFraction(densityFrac);
    }
    return hausdorffDistanceObj.distance();
  }

  public static Geometry addMeasure(Geometry geom, double measure_start, double measure_end) {
    if (!(geom instanceof LineString) && !(geom instanceof MultiLineString)) {
      throw new IllegalArgumentException("Geometry must be a LineString or MultiLineString.");
    }

    if (geom instanceof LineString) {
      return addMeasure((LineString) geom, measure_start, measure_end);
    } else { // MultiLineString
      return addMeasure((MultiLineString) geom, measure_start, measure_end);
    }
  }

  private static Geometry addMeasure(LineString geom, double measureStart, double measureEnd) {
    Coordinate[] coordinates = geom.getCoordinates();
    double totalLength = geom.getLength();
    CoordinateList newCoordinates = new CoordinateList();

    Coordinate c1 = coordinates[0];
    double measure, measureRange = measureEnd - measureStart;
    int numCoordinates = coordinates.length;

    for (int i = 0; i < numCoordinates; i++) {
      Coordinate c2 = coordinates[i];
      double length = c1.distance(c2);

      if (totalLength > 0.0) {
        measure = measureStart + measureRange * (length / totalLength);
      } else if (totalLength == 0.0 && numCoordinates > 1) { // valid zero length LineStrings
        measure = measureStart + measureRange * i / (numCoordinates - 1);
      } else {
        measure = 0.0;
      }

      CoordinateXYZM newCoordinate = new CoordinateXYZM(c2.getX(), c2.getY(), c2.getZ(), measure);
      newCoordinates.add(newCoordinate);
    }

    return geom.getFactory().createLineString(newCoordinates.toCoordinateArray());
  }

  private static Geometry addMeasure(MultiLineString geom, double measureStart, double measureEnd) {
    double totalLength = 0, measureRange = measureEnd - measureStart;
    for (int i = 0; i < geom.getNumGeometries(); i++) {
      LineString linestring = (LineString) geom.getGeometryN(i);
      if (linestring.getCoordinates().length > 1) totalLength += linestring.getLength();
    }
    LineString[] newLineStrings = new LineString[geom.getNumGeometries()];
    double length = 0;
    for (int i = 0; i < geom.getNumGeometries(); i++) {
      double subMeasureStart, subMeasureEnd, subLength = 0;
      LineString lineString = (LineString) geom.getGeometryN(i);
      if (lineString.getCoordinates().length > 1) subLength = lineString.getCoordinates().length;

      subMeasureStart = measureStart + measureRange * length / totalLength;
      subMeasureEnd = measureStart + measureRange * (length + subLength) / totalLength;

      newLineStrings[i] = (LineString) addMeasure(lineString, subMeasureStart, subMeasureEnd);

      length += subLength;
    }

    return geom.getFactory().createMultiLineString(newLineStrings);
  }

  public static Boolean isMeasuredGeometry(Geometry geom) {
    Coordinate coordinate = geom.getCoordinate();
    return !Double.isNaN(coordinate.getM());
  }

  /**
   * Returns a geometry that does not cross the anti meridian. If the given geometry crosses the
   * anti-meridian, it will be split up into multiple geometries.
   *
   * @param geom the geometry to convert
   * @return a geometry that does not cross the anti meridian
   */
  public static Geometry antiMeridianSafeGeom(Geometry geom) {
    try {
      JtsGeometry jtsGeom = new JtsGeometry(geom, JtsSpatialContext.GEO, true, true);
      return jtsGeom.getGeom();
    } catch (TopologyException e) {
      return geom;
    }
  }
}
