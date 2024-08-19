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
package org.apache.sedona.common;

import static com.google.common.geometry.S2.DBL_EPSILON;
import static org.apache.sedona.common.FunctionsGeoTools.bufferSpheroid;

import com.google.common.geometry.S2CellId;
import com.uber.h3core.exceptions.H3Exception;
import com.uber.h3core.util.LatLng;
import java.util.*;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.sedona.common.geometryObjects.Circle;
import org.apache.sedona.common.sphere.Spheroid;
import org.apache.sedona.common.subDivide.GeometrySubDivider;
import org.apache.sedona.common.utils.*;
import org.locationtech.jts.algorithm.MinimumBoundingCircle;
import org.locationtech.jts.algorithm.Orientation;
import org.locationtech.jts.algorithm.construct.LargestEmptyCircle;
import org.locationtech.jts.algorithm.construct.MaximumInscribedCircle;
import org.locationtech.jts.algorithm.hull.ConcaveHull;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.util.AffineTransformation;
import org.locationtech.jts.geom.util.GeometryFixer;
import org.locationtech.jts.io.ByteOrderValues;
import org.locationtech.jts.io.gml2.GMLWriter;
import org.locationtech.jts.io.kml.KMLWriter;
import org.locationtech.jts.linearref.LengthIndexedLine;
import org.locationtech.jts.operation.buffer.BufferOp;
import org.locationtech.jts.operation.buffer.BufferParameters;
import org.locationtech.jts.operation.distance.DistanceOp;
import org.locationtech.jts.operation.distance3d.Distance3DOp;
import org.locationtech.jts.operation.linemerge.LineMerger;
import org.locationtech.jts.operation.overlay.snap.GeometrySnapper;
import org.locationtech.jts.operation.polygonize.Polygonizer;
import org.locationtech.jts.operation.union.UnaryUnionOp;
import org.locationtech.jts.operation.valid.IsSimpleOp;
import org.locationtech.jts.operation.valid.IsValidOp;
import org.locationtech.jts.operation.valid.TopologyValidationError;
import org.locationtech.jts.precision.GeometryPrecisionReducer;
import org.locationtech.jts.precision.MinimumClearance;
import org.locationtech.jts.simplify.PolygonHullSimplifier;
import org.locationtech.jts.simplify.TopologyPreservingSimplifier;
import org.locationtech.jts.simplify.VWSimplifier;
import org.locationtech.jts.triangulate.DelaunayTriangulationBuilder;
import org.locationtech.jts.triangulate.polygon.ConstrainedDelaunayTriangulator;
import org.wololo.geojson.Feature;
import org.wololo.geojson.FeatureCollection;
import org.wololo.jts2geojson.GeoJSONWriter;

public class Functions {
  private static final double DEFAULT_TOLERANCE = 1e-6;
  private static final int DEFAULT_MAX_ITER = 1000;
  private static final int OGC_SFS_VALIDITY = 0; // Use usual OGC SFS validity semantics
  private static final int ESRI_VALIDITY = 1; // ESRI validity model

  public static double area(Geometry geometry) {
    return geometry.getArea();
  }

  public static double azimuth(Geometry left, Geometry right) {
    Coordinate leftCoordinate = left.getCoordinate();
    Coordinate rightCoordinate = right.getCoordinate();
    double deltaX = rightCoordinate.x - leftCoordinate.x;
    double deltaY = rightCoordinate.y - leftCoordinate.y;
    double azimuth = Math.atan2(deltaX, deltaY);
    return azimuth < 0 ? azimuth + (2 * Math.PI) : azimuth;
  }

  public static Geometry boundary(Geometry geometry) {
    Geometry boundary = geometry.getBoundary();
    if (boundary instanceof LinearRing) {
      boundary = geometry.getFactory().createLineString(boundary.getCoordinates());
    }
    return boundary;
  }

  public static Geometry expand(Geometry geometry, double uniformDelta) {
    return expand(geometry, uniformDelta, uniformDelta, uniformDelta);
  }

  public static Geometry expand(Geometry geometry, double deltaX, double deltaY) {
    return expand(geometry, deltaX, deltaY, 0);
  }

  public static Geometry expand(Geometry geometry, double deltaX, double deltaY, double deltaZ) {
    if (geometry == null || geometry.isEmpty()) {
      return geometry;
    }

    Coordinate[] coordinates = geometry.getCoordinates();
    double minX = Double.MAX_VALUE;
    double maxX = -Double.MAX_VALUE;
    double minY = Double.MAX_VALUE;
    double maxY = -Double.MAX_VALUE;
    double minZ = Double.MAX_VALUE;
    double maxZ = -Double.MAX_VALUE;

    for (int i = 0; i < coordinates.length; i++) {
      minX = Math.min(minX, coordinates[i].x);
      maxX = Math.max(maxX, coordinates[i].x);
      minY = Math.min(minY, coordinates[i].y);
      maxY = Math.max(maxY, coordinates[i].y);
      minZ = Math.min(minZ, coordinates[i].z);
      maxZ = Math.max(maxZ, coordinates[i].z);
    }

    minX = minX - deltaX;
    maxX = maxX + deltaX;
    minY = minY - deltaY;
    maxY = maxY + deltaY;

    if (Functions.hasZ(geometry)) {
      minZ = minZ - deltaZ;
      maxZ = maxZ + deltaZ;
      Coordinate[] newCoords = new Coordinate[5];
      newCoords[0] = new Coordinate(minX, minY, minZ);
      newCoords[1] = new Coordinate(minX, maxY, minZ);
      newCoords[2] = new Coordinate(maxX, maxY, maxZ);
      newCoords[3] = new Coordinate(maxX, minY, maxZ);
      newCoords[4] = newCoords[0];
      return geometry.getFactory().createPolygon(newCoords);
    }
    Geometry result = Constructors.polygonFromEnvelope(minX, minY, maxX, maxY);
    result.setSRID(geometry.getSRID());
    return result;
  }

  public static Geometry buffer(Geometry geometry, double radius) {
    return buffer(geometry, radius, false, "");
  }

  public static Geometry buffer(Geometry geometry, double radius, boolean useSpheroid) {
    return buffer(geometry, radius, useSpheroid, "");
  }

  public static Geometry buffer(
      Geometry geometry, double radius, boolean useSpheroid, String params)
      throws IllegalArgumentException {
    BufferParameters bufferParameters = new BufferParameters();

    // Processing parameters
    if (!params.isEmpty()) {
      bufferParameters = parseBufferParams(params);

      // convert the sign to the appropriate direction
      // left - radius should be positive
      // right - radius should be negative
      if (bufferParameters.isSingleSided()
          && (params.toLowerCase().contains("left") && radius < 0
              || params.toLowerCase().contains("right") && radius > 0)) {
        radius = -radius;
      }
    }

    if (useSpheroid) {
      // Spheroidal buffering logic
      try {
        return bufferSpheroid(geometry, radius, bufferParameters);
      } catch (RuntimeException e) {
        throw new RuntimeException("Error processing spheroidal buffer", e);
      }
    } else {
      // Existing planar buffer logic with params handling
      return BufferOp.bufferOp(geometry, radius, bufferParameters);
    }
  }

  private static BufferParameters parseBufferParams(String params) {

    String[] listBufferParameters = {
      "quad_segs", "endcap", "join", "mitre_limit", "miter_limit", "side"
    };
    String[] endcapOptions = {"round", "flat", "butt", "square"};
    String[] joinOptions = {"round", "mitre", "miter", "bevel"};
    String[] sideOptions = {"both", "left", "right"};

    BufferParameters bufferParameters = new BufferParameters();
    String[] listParams = params.split(" ");

    for (String param : listParams) {
      String[] singleParam = param.split("=");

      if (singleParam.length != 2) {
        throw new IllegalArgumentException(
            String.format(
                "%s is not the valid format. The valid format is key=value, for example `endcap=butt quad_segs=4`.",
                param));
      }

      // Set quadrant segment
      if (singleParam[0].equalsIgnoreCase(listBufferParameters[0])) {
        try {
          bufferParameters.setQuadrantSegments(Integer.parseInt(singleParam[1]));
        } catch (NumberFormatException e) {
          throw new IllegalArgumentException(
              String.format(
                  "%1$s is not an integer. Quadrant segment should be an integer.",
                  singleParam[1]));
        }
      }
      // Set end cap style
      else if (singleParam[0].equalsIgnoreCase(listBufferParameters[1])) {
        if (singleParam[1].equalsIgnoreCase(endcapOptions[0])) {
          bufferParameters.setEndCapStyle(BufferParameters.CAP_ROUND);
        } else if (singleParam[1].equalsIgnoreCase(endcapOptions[1])
            || singleParam[1].equalsIgnoreCase(endcapOptions[2])) {
          bufferParameters.setEndCapStyle(BufferParameters.CAP_FLAT);
        } else if (singleParam[1].equalsIgnoreCase(endcapOptions[3])) {
          bufferParameters.setEndCapStyle(BufferParameters.CAP_SQUARE);
        } else {
          throw new IllegalArgumentException(
              String.format(
                  "%s is not a valid option. Accepted options are %s.",
                  singleParam[1], Arrays.toString(endcapOptions)));
        }
      }
      // Set join style
      else if (singleParam[0].equalsIgnoreCase(listBufferParameters[2])) {
        if (singleParam[1].equalsIgnoreCase(joinOptions[0])) {
          bufferParameters.setJoinStyle(BufferParameters.JOIN_ROUND);
        } else if (singleParam[1].equalsIgnoreCase(joinOptions[1])
            || singleParam[1].equalsIgnoreCase(joinOptions[2])) {
          bufferParameters.setJoinStyle(BufferParameters.JOIN_MITRE);
        } else if (singleParam[1].equalsIgnoreCase(joinOptions[3])) {
          bufferParameters.setJoinStyle(BufferParameters.JOIN_BEVEL);
        } else {
          throw new IllegalArgumentException(
              String.format(
                  "%s is not a valid option. Accepted options are %s",
                  singleParam[1], Arrays.toString(joinOptions)));
        }
      }
      // Set mitre ratio limit
      else if (singleParam[0].equalsIgnoreCase(listBufferParameters[3])
          || singleParam[0].equalsIgnoreCase(listBufferParameters[4])) {
        try {
          bufferParameters.setMitreLimit(Double.parseDouble(singleParam[1]));
        } catch (NumberFormatException e) {
          throw new IllegalArgumentException(
              String.format(
                  "%1$s is not a double. Mitre limit should be a double.", singleParam[1]));
        }
        continue;
      }
      // Set side to add buffer
      else if (singleParam[0].equalsIgnoreCase(listBufferParameters[5])) {
        if (singleParam[1].equalsIgnoreCase(sideOptions[0])) {
          // It defaults to square end cap style when side is specified
          bufferParameters.setEndCapStyle(BufferParameters.CAP_SQUARE);
          continue;
        } else if (singleParam[1].equalsIgnoreCase(sideOptions[1])
            || singleParam[1].equalsIgnoreCase(sideOptions[2])) {
          bufferParameters.setSingleSided(true);
        } else {
          throw new IllegalArgumentException(
              String.format(
                  "%s is not a valid option. Accepted options are %s ",
                  singleParam[1], Arrays.toString(sideOptions)));
        }
      }
      // everything else
      else {
        throw new IllegalArgumentException(
            String.format(
                "%s is not a valid style parameter. Accepted style parameters are %s.",
                singleParam[0], Arrays.toString(listBufferParameters)));
      }
    }
    return bufferParameters;
  }

  public static int bestSRID(Geometry geometry) throws IllegalArgumentException {
    // Shift longitudes if geometry crosses dateline
    if (crossesDateLine(geometry)) {
      shiftLongitude(geometry);
    }

    // Check envelope
    Envelope envelope = geometry.getEnvelopeInternal();
    if (envelope.isNull()) return Spheroid.EPSG_WORLD_MERCATOR; // Fallback EPSG

    // Calculate the center of the envelope
    double centerX = (envelope.getMinX() + envelope.getMaxX()) / 2.0; // centroid.getX();
    double centerY = (envelope.getMinY() + envelope.getMaxY()) / 2.0; // centroid.getY();

    // Calculate angular width and height
    Double xwidth = Spheroid.angularWidth(envelope);
    Double ywidth = Spheroid.angularHeight(envelope);
    if (xwidth.isNaN() | ywidth.isNaN()) {
      throw new IllegalArgumentException(
          "Only lon/lat coordinate systems are supported by ST_BestSRID");
    }

    // Prioritize polar regions for Lambert Azimuthal Equal Area projection
    if (centerY >= 70.0 && ywidth < 45.0) return Spheroid.EPSG_NORTH_LAMBERT;
    if (centerY <= -70.0 && ywidth < 45.0) return Spheroid.EPSG_SOUTH_LAMBERT;

    // Check for UTM zones
    if (xwidth < 6.0) {
      int zone;
      if (centerX == -180.0 || centerX == 180.0) {
        zone = 59; // UTM zone 60
      } else {
        zone = (int) Math.floor((centerX + 180.0) / 6.0);
        zone = (zone > 59) ? zone - 60 : zone;
      }
      return (centerY < 0.0)
          ? Spheroid.EPSG_SOUTH_UTM_START + zone
          : Spheroid.EPSG_NORTH_UTM_START + zone;
    }

    // Default fallback to Mercator projection
    return Spheroid.EPSG_WORLD_MERCATOR;
  }

  /**
   * Corrects the longitudes of a geometry to be within the -180 to 180 range. This method modifies
   * the original geometry.
   *
   * @param geometry The geometry to be corrected.
   */
  public static void normalizeLongitude(Geometry geometry) {
    if (geometry == null || geometry.isEmpty()) {
      return;
    }

    geometry.apply(
        new CoordinateSequenceFilter() {
          @Override
          public void filter(CoordinateSequence seq, int i) {
            double x = seq.getX(i);
            // Normalize the longitude to be within -180 to 180 range
            while (x > 180) x -= 360;
            while (x < -180) x += 360;
            seq.setOrdinate(i, CoordinateSequence.X, x);
          }

          @Override
          public boolean isDone() {
            return false; // Continue processing until all coordinates are processed
          }

          @Override
          public boolean isGeometryChanged() {
            return true; // The geometry is changed as we are modifying the coordinates
          }
        });

    geometry.geometryChanged(); // Notify the geometry that its coordinates have been changed
  }

  /**
   * Checks if a geometry crosses the International Date Line.
   *
   * @param geometry The geometry to check.
   * @return True if the geometry crosses the Date Line, false otherwise.
   */
  public static boolean crossesDateLine(Geometry geometry) {
    if (geometry == null || geometry.isEmpty()) {
      return false;
    }

    CoordinateSequenceFilter filter =
        new CoordinateSequenceFilter() {
          private Coordinate previous = null;
          private boolean crossesDateLine = false;

          @Override
          public void filter(CoordinateSequence seq, int i) {
            if (i == 0) {
              previous = seq.getCoordinateCopy(i);
              return;
            }

            Coordinate current = seq.getCoordinateCopy(i);
            if (Math.abs(current.x - previous.x) > 180) {
              crossesDateLine = true;
            }

            previous = current;
          }

          @Override
          public boolean isDone() {
            return crossesDateLine;
          }

          @Override
          public boolean isGeometryChanged() {
            return false;
          }
        };

    geometry.apply(filter);
    return filter.isDone();
  }

  public static Geometry shiftLongitude(Geometry geometry) {
    geometry.apply(
        new CoordinateSequenceFilter() {
          @Override
          public void filter(CoordinateSequence seq, int i) {
            double newX = seq.getX(i);
            if (newX < 0) {
              newX += 360;
            } else if (newX > 180) {
              newX -= 360;
            }
            seq.setOrdinate(i, CoordinateSequence.X, newX);
          }

          @Override
          public boolean isDone() {
            return false;
          }

          @Override
          public boolean isGeometryChanged() {
            return true;
          }
        });
    return geometry;
  }

  public static Geometry envelope(Geometry geometry) {
    return geometry.getEnvelope();
  }

  public static double distance(Geometry left, Geometry right) {
    return left.distance(right);
  }

  public static double distance3d(Geometry left, Geometry right) {
    return new Distance3DOp(left, right).distance();
  }

  public static double length(Geometry geometry) {
    return geometry.getLength();
  }

  public static Geometry normalize(Geometry geometry) {
    geometry.normalize();
    return geometry;
  }

  public static Double x(Geometry geometry) {
    if (geometry instanceof Point) {
      return geometry.getCoordinate().x;
    }
    return null;
  }

  public static Double y(Geometry geometry) {
    if (geometry instanceof Point) {
      return geometry.getCoordinate().y;
    }
    return null;
  }

  public static Double z(Geometry geometry) {
    if (geometry instanceof Point) {
      return geometry.getCoordinate().z;
    }
    return null;
  }

  public static Double m(Geometry geom) {
    if (geom instanceof Point) {
      return geom.getCoordinate().getM();
    }
    return null;
  }

  public static Double mMin(Geometry geometry) {
    Coordinate[] points = geometry.getCoordinates();
    double min = Double.MAX_VALUE;
    for (int i = 0; i < points.length; i++) {
      if (Double.isNaN(points[i].getM())) continue;
      min = Math.min(points[i].getM(), min);
    }
    return min == Double.MAX_VALUE ? null : min;
  }

  public static Double mMax(Geometry geometry) {
    Coordinate[] points = geometry.getCoordinates();
    double max = -Double.MAX_VALUE;
    for (int i = 0; i < points.length; i++) {
      if (Double.isNaN(points[i].getM())) continue;
      max = Math.max(points[i].getM(), max);
    }
    return max == -Double.MAX_VALUE ? null : max;
  }

  public static double xMin(Geometry geometry) {
    Coordinate[] points = geometry.getCoordinates();
    double min = Double.MAX_VALUE;
    for (int i = 0; i < points.length; i++) {
      min = Math.min(points[i].getX(), min);
    }
    return min;
  }

  public static double xMax(Geometry geometry) {
    Coordinate[] points = geometry.getCoordinates();
    double max = -Double.MAX_VALUE;
    for (int i = 0; i < points.length; i++) {
      max = Math.max(points[i].getX(), max);
    }
    return max;
  }

  public static double yMin(Geometry geometry) {
    Coordinate[] points = geometry.getCoordinates();
    double min = Double.MAX_VALUE;
    for (int i = 0; i < points.length; i++) {
      min = Math.min(points[i].getY(), min);
    }
    return min;
  }

  public static double yMax(Geometry geometry) {
    Coordinate[] points = geometry.getCoordinates();
    double max = -Double.MAX_VALUE;
    for (int i = 0; i < points.length; i++) {
      max = Math.max(points[i].getY(), max);
    }
    return max;
  }

  public static Double zMax(Geometry geometry) {
    Coordinate[] points = geometry.getCoordinates();
    double max = -Double.MAX_VALUE;
    for (int i = 0; i < points.length; i++) {
      if (Double.isNaN(points[i].getZ())) continue;
      max = Math.max(points[i].getZ(), max);
    }
    return max == -Double.MAX_VALUE ? null : max;
  }

  public static Double zMin(Geometry geometry) {
    Coordinate[] points = geometry.getCoordinates();
    double min = Double.MAX_VALUE;
    for (int i = 0; i < points.length; i++) {
      if (Double.isNaN(points[i].getZ())) continue;
      min = Math.min(points[i].getZ(), min);
    }
    return min == Double.MAX_VALUE ? null : min;
  }

  public static boolean hasM(Geometry geom) {
    Coordinate coord = geom.getCoordinate();
    return !Double.isNaN(coord.getM());
  }

  public static boolean hasZ(Geometry geom) {
    Coordinate coord = geom.getCoordinate();
    return !Double.isNaN(coord.getZ());
  }

  public static Geometry flipCoordinates(Geometry geometry) {
    GeomUtils.flipCoordinates(geometry);
    return geometry;
  }

  public static String geohash(Geometry geometry, int precision) {
    return GeometryGeoHashEncoder.calculate(geometry, precision);
  }

  public static Geometry pointOnSurface(Geometry geometry) {
    return GeomUtils.getInteriorPoint(geometry);
  }

  public static Geometry reverse(Geometry geometry) {
    return geometry.reverse();
  }

  public static Geometry geometryN(Geometry geometry, int n) {
    if (n < geometry.getNumGeometries()) {
      Geometry subGeom = geometry.getGeometryN(n);
      if (subGeom.getSRID() == geometry.getSRID()) {
        return subGeom;
      }
      return setSRID(subGeom, geometry.getSRID());
    }
    return null;
  }

  public static Geometry interiorRingN(Geometry geometry, int n) {
    if (geometry instanceof Polygon) {
      Polygon polygon = (Polygon) geometry;
      if (n < polygon.getNumInteriorRing()) {
        Geometry interiorRing = polygon.getInteriorRingN(n);
        if (interiorRing instanceof LinearRing) {
          interiorRing = polygon.getFactory().createLineString(interiorRing.getCoordinates());
        }
        return interiorRing;
      }
    }
    return null;
  }

  public static Geometry pointN(Geometry geometry, int n) {
    if (!(geometry instanceof LineString)) {
      return null;
    }
    return GeomUtils.getNthPoint((LineString) geometry, n);
  }

  public static Geometry exteriorRing(Geometry geometry) {
    Geometry ring = GeomUtils.getExteriorRing(geometry);
    if (ring instanceof LinearRing) {
      ring = geometry.getFactory().createLineString(ring.getCoordinates());
    }
    return ring;
  }

  public static String asEWKT(Geometry geometry) {
    return GeomUtils.getEWKT(geometry);
  }

  public static String asWKT(Geometry geometry) {
    return GeomUtils.getWKT(geometry);
  }

  public static byte[] asEWKB(Geometry geometry) {
    return GeomUtils.getEWKB(geometry);
  }

  public static String asHexEWKB(Geometry geom, String endian) {
    if (endian.equalsIgnoreCase("NDR")) {
      return GeomUtils.getHexEWKB(geom, ByteOrderValues.LITTLE_ENDIAN);
    } else if (endian.equalsIgnoreCase("XDR")) {
      return GeomUtils.getHexEWKB(geom, ByteOrderValues.BIG_ENDIAN);
    }
    throw new IllegalArgumentException(
        "You must select either NDR (little-endian) or XDR (big-endian) as the endian format.");
  }

  public static String asHexEWKB(Geometry geom) {
    return asHexEWKB(geom, "NDR");
  }

  public static byte[] asWKB(Geometry geometry) {
    return GeomUtils.getWKB(geometry);
  }

  public static String asGeoJson(Geometry geometry) {
    return asGeoJson(geometry, "simple");
  }

  public static String asGeoJson(Geometry geometry, String type) {
    if (geometry == null) {
      return null;
    }

    GeoJSONWriter writer = new GeoJSONWriter();
    org.wololo.geojson.Geometry geoJson = writer.write(geometry);

    switch (type.toLowerCase()) {
      case "simple":
        return geoJson.toString();

      case "feature":
        Map<String, Object> properties = new HashMap<>();
        Feature feature = new Feature(geoJson, properties);
        return feature.toString();

      case "featurecollection":
        List<Feature> features = new ArrayList<>();
        features.add(new Feature(geoJson, new HashMap<>()));
        FeatureCollection featureCollection =
            new FeatureCollection(features.toArray(new Feature[0]));
        return featureCollection.toString();

      default:
        throw new IllegalArgumentException(
            "Unknown type: "
                + type
                + ". Valid types are: 'simple', 'feature', 'featurecollection'.");
    }
  }

  public static int nPoints(Geometry geometry) {
    return geometry.getNumPoints();
  }

  public static int nDims(Geometry geometry) {
    int count_dimension = 0;
    Coordinate geom = geometry.getCoordinate();
    Double x_cord = geom.getX();
    Double y_cord = geom.getY();
    Double z_cord = geom.getZ();
    Double m_cord = geom.getM();
    if (!Double.isNaN(x_cord)) count_dimension++;
    if (!Double.isNaN(y_cord)) count_dimension++;
    if (!Double.isNaN(z_cord)) count_dimension++;
    if (!Double.isNaN(m_cord)) count_dimension++;
    return count_dimension;
  }

  public static int numGeometries(Geometry geometry) {
    return geometry.getNumGeometries();
  }

  public static Integer numInteriorRings(Geometry geometry) {
    if (geometry instanceof Polygon) {
      return ((Polygon) geometry).getNumInteriorRing();
    }
    return null;
  }

  public static String asGML(Geometry geometry) {
    return new GMLWriter().write(geometry);
  }

  public static String asKML(Geometry geometry) {
    return new KMLWriter().write(geometry);
  }

  public static Geometry force2D(Geometry geometry) {
    return GeomUtils.get2dGeom(geometry);
  }

  public static boolean isEmpty(Geometry geometry) {
    return geometry.isEmpty();
  }

  public static Geometry buildArea(Geometry geometry) {
    return GeomUtils.buildArea(geometry);
  }

  public static Geometry setSRID(Geometry geometry, int srid) {
    if (geometry == null) {
      return null;
    }
    GeometryFactory factory =
        new GeometryFactory(
            geometry.getPrecisionModel(),
            srid,
            geometry.getFactory().getCoordinateSequenceFactory());
    return factory.createGeometry(geometry);
  }

  public static int getSRID(Geometry geometry) {
    if (geometry == null) {
      return 0;
    }
    return geometry.getSRID();
  }

  public static boolean isClosed(Geometry geometry) {
    if (geometry instanceof Circle
        || geometry instanceof MultiPoint
        || geometry instanceof MultiPolygon
        || geometry instanceof Point
        || geometry instanceof Polygon) {
      return true;
    } else if (geometry instanceof LineString) {
      return ((LineString) geometry).isClosed();
    } else if (geometry instanceof MultiLineString) {
      return ((MultiLineString) geometry).isClosed();
    } else if (geometry instanceof GeometryCollection) {
      return false;
    }
    return false;
  }

  public static boolean isRing(Geometry geometry) {
    return geometry instanceof LineString
        && ((LineString) geometry).isClosed()
        && geometry.isSimple();
  }

  public static boolean isSimple(Geometry geometry) {
    return new IsSimpleOp(geometry).isSimple();
  }

  public static boolean isValid(Geometry geometry) {
    return isValid(geometry, OGC_SFS_VALIDITY);
  }

  public static boolean isValid(Geometry geom, int flag) {
    IsValidOp isValidOp = new IsValidOp(geom);

    // Set the validity model based on flags
    if (flag == ESRI_VALIDITY) {
      isValidOp.setSelfTouchingRingFormingHoleValid(true);
    } else {
      isValidOp.setSelfTouchingRingFormingHoleValid(false);
    }

    return isValidOp.isValid();
  }

  public static boolean isValidTrajectory(Geometry geom) {
    if (!(geom.getClass().getSimpleName().equals("LineString"))) {
      return false;
    }

    if (!Functions.hasM(geom)) {
      return false;
    }

    double measure = -1 * Double.MAX_VALUE;
    Coordinate[] coordinates = geom.getCoordinates();
    for (int i = 0; i < geom.getNumPoints(); i++) {
      if (coordinates[i].getM() <= measure) {
        return false;
      }
      measure = coordinates[i].getM();
    }
    return true;
  }

  public static Geometry addPoint(Geometry linestring, Geometry point) {
    return addPoint(linestring, point, -1);
  }

  public static Geometry addPoint(Geometry linestring, Geometry point, int position) {
    if (linestring instanceof LineString && point instanceof Point) {
      List<Coordinate> coordinates = new ArrayList<>(Arrays.asList(linestring.getCoordinates()));
      if (-1 <= position && position <= coordinates.size()) {
        if (position < 0) {
          coordinates.add(point.getCoordinate());
        } else {
          coordinates.add(position, point.getCoordinate());
        }
        return linestring.getFactory().createLineString(coordinates.toArray(new Coordinate[0]));
      }
    }
    return null;
  }

  public static Geometry removePoint(Geometry linestring) {
    if (linestring != null) {
      return removePoint(linestring, -1);
    }
    return null;
  }

  public static Geometry removePoint(Geometry linestring, int position) {
    if (linestring instanceof LineString) {
      List<Coordinate> coordinates = new ArrayList<>(Arrays.asList(linestring.getCoordinates()));
      if (2 < coordinates.size() && position < coordinates.size()) {
        if (position == -1) {
          position = coordinates.size() - 1;
        }
        coordinates.remove(position);
        return linestring.getFactory().createLineString(coordinates.toArray(new Coordinate[0]));
      }
    }
    return null;
  }

  public static Geometry setPoint(Geometry linestring, int position, Geometry point) {
    if (linestring instanceof LineString) {
      List<Coordinate> coordinates = new ArrayList<>(Arrays.asList(linestring.getCoordinates()));
      if (-coordinates.size() <= position && position < coordinates.size()) {
        if (position < 0) {
          coordinates.set(coordinates.size() + position, point.getCoordinate());
        } else {
          coordinates.set(position, point.getCoordinate());
        }
        return linestring.getFactory().createLineString(coordinates.toArray(new Coordinate[0]));
      }
    }
    return null;
  }

  public static Geometry lineFromMultiPoint(Geometry geometry) {
    if (!(geometry instanceof MultiPoint)) {
      return null;
    }
    List<Coordinate> coordinates = new ArrayList<>();
    for (Coordinate c : geometry.getCoordinates()) {
      coordinates.add(c);
    }
    return geometry.getFactory().createLineString(coordinates.toArray(new Coordinate[0]));
  }

  public static Geometry closestPoint(Geometry left, Geometry right) {
    DistanceOp distanceOp = new DistanceOp(left, right);
    try {
      Coordinate[] closestPoints = distanceOp.nearestPoints();
      return left.getFactory().createPoint(closestPoints[0]);
    } catch (Exception e) {
      throw new IllegalArgumentException("ST_ClosestPoint doesn't support empty geometry object.");
    }
  }

  public static Geometry delaunayTriangle(Geometry geometry) {
    return delaunayTriangle(geometry, 0.0, 0);
  }

  public static Geometry delaunayTriangle(Geometry geometry, double tolerance) {
    return delaunayTriangle(geometry, tolerance, 0);
  }

  public static Geometry delaunayTriangle(Geometry geometry, double tolerance, int flag) {
    DelaunayTriangulationBuilder dTBuilder = new DelaunayTriangulationBuilder();
    dTBuilder.setSites(geometry);
    dTBuilder.setTolerance(tolerance);
    if (flag == 0) {
      return dTBuilder.getTriangles(geometry.getFactory());
    } else if (flag == 1) {
      return dTBuilder.getEdges(geometry.getFactory());
    } else {
      throw new IllegalArgumentException("Select a valid flag option (0 or 1).");
    }
  }

  public static int zmFlag(Geometry geom) {
    Coordinate coords = geom.getCoordinate();
    boolean hasZ = !Double.isNaN(coords.getZ());
    boolean hasM = !Double.isNaN(coords.getM());
    if (hasM && hasZ) {
      // geom is 4D
      return 3;
    } else if (hasZ) {
      // geom is 3D-Z
      return 2;
    } else if (hasM) {
      // geom is 3D-M
      return 1;
    }
    // geom is 2D
    return 0;
  }

  public static Geometry concaveHull(Geometry geometry, double pctConvex, boolean allowHoles) {
    ConcaveHull concave_hull = new ConcaveHull(geometry);
    concave_hull.setMaximumEdgeLengthRatio(pctConvex);
    concave_hull.setHolesAllowed(allowHoles);

    return concave_hull.getHull();
  }

  public static Geometry convexHull(Geometry geometry) {
    return geometry.convexHull();
  }

  public static Geometry getCentroid(Geometry geometry) {
    return geometry.getCentroid();
  }

  public static Geometry intersection(Geometry leftGeometry, Geometry rightGeometry) {
    boolean isIntersects = leftGeometry.intersects(rightGeometry);
    if (!isIntersects) {
      return leftGeometry.getFactory().createPolygon();
    }
    if (leftGeometry.contains(rightGeometry)) {
      return rightGeometry;
    }
    if (rightGeometry.contains(leftGeometry)) {
      return leftGeometry;
    }
    return leftGeometry.intersection(rightGeometry);
  }

  public static Geometry makeValid(Geometry geometry, boolean keepCollapsed) {
    GeometryFixer fixer = new GeometryFixer(geometry);
    fixer.setKeepCollapsed(keepCollapsed);
    return fixer.getResult();
  }

  public static Geometry reducePrecision(Geometry geometry, int precisionScale) {
    GeometryPrecisionReducer precisionReduce =
        new GeometryPrecisionReducer(new PrecisionModel(Math.pow(10, precisionScale)));
    return precisionReduce.reduce(geometry);
  }

  public static Geometry lineMerge(Geometry geometry) {
    if (geometry instanceof MultiLineString) {
      MultiLineString multiLineString = (MultiLineString) geometry;
      int numLineStrings = multiLineString.getNumGeometries();
      LineMerger merger = new LineMerger();
      for (int k = 0; k < numLineStrings; k++) {
        LineString line = (LineString) multiLineString.getGeometryN(k);
        merger.add(line);
      }
      if (merger.getMergedLineStrings().size() == 1) {
        // If the merger was able to join the lines, there will be only one element
        return (Geometry) merger.getMergedLineStrings().iterator().next();
      } else {
        // if the merger couldn't join the lines, it will contain the individual lines, so return
        // the input
        return geometry;
      }
    }
    return geometry.getFactory().createGeometryCollection();
  }

  public static Geometry minimumBoundingCircle(Geometry geometry, int quadrantSegments) {
    MinimumBoundingCircle minimumBoundingCircle = new MinimumBoundingCircle(geometry);
    Coordinate centre = minimumBoundingCircle.getCentre();
    double radius = minimumBoundingCircle.getRadius();
    Geometry circle = null;
    if (centre == null) {
      circle = geometry.getFactory().createPolygon();
    } else {
      circle = geometry.getFactory().createPoint(centre);
      if (radius != 0D) {
        circle = circle.buffer(radius, quadrantSegments);
      }
    }
    return circle;
  }

  public static InscribedCircle maximumInscribedCircle(Geometry geometry) {
    // Calculating the tolerance
    Envelope envelope = geometry.getEnvelopeInternal();
    double width = envelope.getWidth(), height = envelope.getHeight(), size, tolerance;
    size = Math.max(width, height);
    tolerance = size / 1000.0;

    Geometry center, nearest;
    double radius;

    // All non-polygonal geometries use LargestEmptyCircle
    if (!geometry.getClass().getSimpleName().equals("Polygon")
        && !geometry.getClass().getSimpleName().equals("MultiPolygon")) {
      LargestEmptyCircle largestEmptyCircle = new LargestEmptyCircle(geometry, tolerance);
      center = largestEmptyCircle.getCenter();
      nearest = largestEmptyCircle.getRadiusPoint();
      radius = largestEmptyCircle.getRadiusLine().getLength();
      return new InscribedCircle(center, nearest, radius);
    }

    MaximumInscribedCircle maximumInscribedCircle = new MaximumInscribedCircle(geometry, tolerance);
    center = maximumInscribedCircle.getCenter();
    nearest = maximumInscribedCircle.getRadiusPoint();
    radius = maximumInscribedCircle.getRadiusLine().getLength();
    return new InscribedCircle(center, nearest, radius);
  }

  public static Pair<Geometry, Double> minimumBoundingRadius(Geometry geometry) {
    MinimumBoundingCircle minimumBoundingCircle = new MinimumBoundingCircle(geometry);
    Coordinate coods = minimumBoundingCircle.getCentre();
    double radius = minimumBoundingCircle.getRadius();
    Point centre = geometry.getFactory().createPoint(coods);
    return Pair.of(centre, radius);
  }

  public static double minimumClearance(Geometry geometry) {
    return MinimumClearance.getDistance(geometry);
  }

  public static Geometry minimumClearanceLine(Geometry geometry) {
    return MinimumClearance.getLine(geometry);
  }

  public static Geometry lineSubString(Geometry geom, double fromFraction, double toFraction) {
    double length = geom.getLength();
    LengthIndexedLine indexedLine = new LengthIndexedLine(geom);
    Geometry subLine = indexedLine.extractLine(length * fromFraction, length * toFraction);
    return subLine;
  }

  public static Geometry lineInterpolatePoint(Geometry geom, double fraction) {
    double length = geom.getLength();
    LengthIndexedLine indexedLine = new LengthIndexedLine(geom);
    Coordinate interPoint = indexedLine.extractPoint(length * fraction);
    return geom.getFactory().createPoint(interPoint);
  }

  /**
   * Forces a Polygon/MultiPolygon to use clockwise orientation for the exterior ring and a
   * counter-clockwise for the interior ring(s).
   *
   * @param geom
   * @return a clockwise orientated (Multi)Polygon
   */
  public static Geometry forcePolygonCW(Geometry geom) {
    if (isPolygonCW(geom)) {
      return geom;
    }

    if (geom instanceof Polygon) {
      return transformCW((Polygon) geom);

    } else if (geom instanceof MultiPolygon) {
      List<Polygon> polygons = new ArrayList<>();
      for (int i = 0; i < geom.getNumGeometries(); i++) {
        Polygon polygon = (Polygon) geom.getGeometryN(i);
        polygons.add((Polygon) transformCW(polygon));
      }

      return geom.getFactory().createMultiPolygon(polygons.toArray(new Polygon[0]));
    }
    // Non-polygonal geometries are returned unchanged
    return geom;
  }

  private static Geometry transformCW(Polygon polygon) {
    LinearRing exteriorRing = polygon.getExteriorRing();
    LinearRing exteriorRingEnforced = transformCW(exteriorRing, true);

    List<LinearRing> interiorRings = new ArrayList<>();
    for (int i = 0; i < polygon.getNumInteriorRing(); i++) {
      interiorRings.add(transformCW(polygon.getInteriorRingN(i), false));
    }

    return polygon
        .getFactory()
        .createPolygon(exteriorRingEnforced, interiorRings.toArray(new LinearRing[0]));
  }

  private static LinearRing transformCW(LinearRing ring, boolean isExteriorRing) {
    boolean isRingClockwise = !Orientation.isCCW(ring.getCoordinateSequence());

    LinearRing enforcedRing;
    if (isExteriorRing) {
      enforcedRing = isRingClockwise ? (LinearRing) ring.copy() : ring.reverse();
    } else {
      enforcedRing = isRingClockwise ? ring.reverse() : (LinearRing) ring.copy();
    }
    return enforcedRing;
  }

  /**
   * This function accepts Polygon and MultiPolygon, if any other type is provided then it will
   * return false. If the exterior ring is clockwise and the interior ring(s) are counter-clockwise
   * then returns true, otherwise false.
   *
   * @param geom Polygon or MultiPolygon
   * @return
   */
  public static boolean isPolygonCW(Geometry geom) {
    if (geom instanceof MultiPolygon) {
      MultiPolygon multiPolygon = (MultiPolygon) geom;

      boolean arePolygonsCW = checkIfPolygonCW((Polygon) multiPolygon.getGeometryN(0));
      for (int i = 1; i < multiPolygon.getNumGeometries(); i++) {
        arePolygonsCW = arePolygonsCW && checkIfPolygonCW((Polygon) multiPolygon.getGeometryN(i));
      }
      return arePolygonsCW;
    } else if (geom instanceof Polygon) {
      return checkIfPolygonCW((Polygon) geom);
    }
    // False for remaining geometry types
    return false;
  }

  private static boolean checkIfPolygonCW(Polygon geom) {
    LinearRing exteriorRing = geom.getExteriorRing();
    boolean isExteriorRingCW = !Orientation.isCCW(exteriorRing.getCoordinateSequence());

    if (geom.getNumInteriorRing() == 0) {
      return isExteriorRingCW;
    }

    boolean isInteriorRingCW = Orientation.isCCW(geom.getInteriorRingN(0).getCoordinateSequence());
    for (int i = 1; i < geom.getNumInteriorRing(); i++) {
      isInteriorRingCW =
          isInteriorRingCW && Orientation.isCCW(geom.getInteriorRingN(i).getCoordinateSequence());
    }

    return isExteriorRingCW && isInteriorRingCW;
  }

  public static Geometry triangulatePolygon(Geometry geom) {
    return ConstrainedDelaunayTriangulator.triangulate(geom);
  }

  public static double lineLocatePoint(Geometry geom, Geometry point) {
    double length = geom.getLength();
    LengthIndexedLine indexedLine = new LengthIndexedLine(geom);
    return indexedLine.indexOf(point.getCoordinate()) / length;
  }

  public static Geometry locateAlong(Geometry linear, double measure, double offset) {
    return GeometryLocateAlongProcessor.processGeometry(linear, measure, offset);
  }

  public static Geometry locateAlong(Geometry linear, double measure) {
    return locateAlong(linear, measure, 0);
  }

  /**
   * Forces a Polygon/MultiPolygon to use counter-clockwise orientation for the exterior ring and a
   * clockwise for the interior ring(s).
   *
   * @param geom
   * @return a counter-clockwise orientated (Multi)Polygon
   */
  public static Geometry forcePolygonCCW(Geometry geom) {
    if (isPolygonCCW(geom)) {
      return geom;
    }

    if (geom instanceof Polygon) {
      return transformCCW((Polygon) geom);

    } else if (geom instanceof MultiPolygon) {
      List<Polygon> polygons = new ArrayList<>();
      for (int i = 0; i < geom.getNumGeometries(); i++) {
        Polygon polygon = (Polygon) geom.getGeometryN(i);
        polygons.add((Polygon) transformCCW(polygon));
      }

      return geom.getFactory().createMultiPolygon(polygons.toArray(new Polygon[0]));
    }
    // Non-polygonal geometries are returned unchanged
    return geom;
  }

  private static Geometry transformCCW(Polygon polygon) {
    LinearRing exteriorRing = polygon.getExteriorRing();
    LinearRing exteriorRingEnforced = transformCCW(exteriorRing, true);

    List<LinearRing> interiorRings = new ArrayList<>();
    for (int i = 0; i < polygon.getNumInteriorRing(); i++) {
      interiorRings.add(transformCCW(polygon.getInteriorRingN(i), false));
    }

    return polygon
        .getFactory()
        .createPolygon(exteriorRingEnforced, interiorRings.toArray(new LinearRing[0]));
  }

  private static LinearRing transformCCW(LinearRing ring, boolean isExteriorRing) {
    boolean isRingCounterClockwise = Orientation.isCCW(ring.getCoordinateSequence());

    LinearRing enforcedRing;
    if (isExteriorRing) {
      enforcedRing = isRingCounterClockwise ? (LinearRing) ring.copy() : ring.reverse();
    } else {
      enforcedRing = isRingCounterClockwise ? ring.reverse() : (LinearRing) ring.copy();
    }
    return enforcedRing;
  }

  /**
   * This function accepts Polygon and MultiPolygon, if any other type is provided then it will
   * return false. If the exterior ring is counter-clockwise and the interior ring(s) are clockwise
   * then returns true, otherwise false.
   *
   * @param geom Polygon or MultiPolygon
   * @return
   */
  public static boolean isPolygonCCW(Geometry geom) {
    if (geom instanceof MultiPolygon) {
      MultiPolygon multiPolygon = (MultiPolygon) geom;

      boolean arePolygonsCCW = checkIfPolygonCCW(((Polygon) multiPolygon.getGeometryN(0)));
      for (int i = 1; i < multiPolygon.getNumGeometries(); i++) {
        arePolygonsCCW =
            arePolygonsCCW && checkIfPolygonCCW((Polygon) multiPolygon.getGeometryN(i));
      }
      return arePolygonsCCW;
    } else if (geom instanceof Polygon) {
      return checkIfPolygonCCW((Polygon) geom);
    }
    // False for remaining geometry types
    return false;
  }

  private static boolean checkIfPolygonCCW(Polygon geom) {
    LinearRing exteriorRing = geom.getExteriorRing();
    boolean isExteriorRingCCW = Orientation.isCCW(exteriorRing.getCoordinateSequence());

    if (geom.getNumInteriorRing() == 0) {
      return isExteriorRingCCW;
    }

    boolean isInteriorRingCCW =
        !Orientation.isCCW(geom.getInteriorRingN(0).getCoordinateSequence());
    for (int i = 1; i < geom.getNumInteriorRing(); i++) {
      isInteriorRingCCW =
          isInteriorRingCCW && !Orientation.isCCW(geom.getInteriorRingN(i).getCoordinateSequence());
    }

    return isExteriorRingCCW && isInteriorRingCCW;
  }

  public static Geometry addMeasure(Geometry geom, double measure_start, double measure_end) {
    return GeomUtils.addMeasure(geom, measure_start, measure_end);
  }

  public static double maxDistance(Geometry geom1, Geometry geom2) {
    return longestLine(geom1, geom2).getLength();
  }

  public static Geometry longestLine(Geometry geom1, Geometry geom2) {
    double maxLength = -Double.MAX_VALUE;
    Coordinate longestStart = null;
    Coordinate longestEnd = null;

    for (Coordinate coord1 : geom1.getCoordinates()) {
      for (Coordinate coord2 : geom2.getCoordinates()) {
        double length = coord1.distance(coord2);
        if (length > maxLength) {
          maxLength = length;
          longestStart = coord1;
          longestEnd = coord2;
        }
      }
    }
    return geom1.getFactory().createLineString(new Coordinate[] {longestStart, longestEnd});
  }

  public static Geometry difference(Geometry leftGeometry, Geometry rightGeometry) {
    boolean isIntersects = leftGeometry.intersects(rightGeometry);
    if (!isIntersects) {
      return leftGeometry;
    } else if (rightGeometry.contains(leftGeometry)) {
      return leftGeometry.getFactory().createPolygon();
    } else {
      return leftGeometry.difference(rightGeometry);
    }
  }

  public static Geometry split(Geometry input, Geometry blade) {
    // check input geometry
    return new GeometrySplitter(input.getFactory()).split(input, blade);
  }

  public static Integer dimension(Geometry geometry) {
    Integer dimension = geometry.getDimension();
    // unknown dimension such as an empty GEOMETRYCOLLECTION
    if (dimension < 0) {
      dimension = 0;
    }
    return dimension;
  }

  /**
   * get the coordinates of a geometry and transform to Google s2 cell id
   *
   * @param input Geometry
   * @param level integer, minimum level of cells covering the geometry
   * @return List of coordinates
   */
  public static Long[] s2CellIDs(Geometry input, int level) {
    HashSet<S2CellId> cellIds = new HashSet<>();
    List<Geometry> geoms = GeomUtils.extractGeometryCollection(input);
    for (Geometry geom : geoms) {
      try {
        cellIds.addAll(
            S2Utils.s2RegionToCellIDs(S2Utils.toS2Region(geom), 1, level, Integer.MAX_VALUE - 1));
      } catch (IllegalArgumentException e) {
        // the geometry can't be cast to region, we cover its coordinates, for example, Point
        cellIds.addAll(
            Arrays.stream(geom.getCoordinates())
                .map(c -> S2Utils.coordinateToCellID(c, level))
                .collect(Collectors.toList()));
      }
    }
    return S2Utils.roundCellsToSameLevel(new ArrayList<>(cellIds), level).stream()
        .map(S2CellId::id)
        .collect(Collectors.toList())
        .toArray(new Long[cellIds.size()]);
  }

  /**
   * @param cellIds array of cell ids
   * @return An array of polygons for the cell ids
   */
  public static Geometry[] s2ToGeom(long[] cellIds) {
    List<S2CellId> s2CellObjs =
        Arrays.stream(cellIds).mapToObj(S2CellId::new).collect(Collectors.toList());
    return s2CellObjs.stream().map(S2Utils::toJTSPolygon).toArray(Polygon[]::new);
  }

  /**
   * cover the geometry with H3 cells
   *
   * @param input the input geometry
   * @param level the resolution of h3 cells
   * @param fullCover whether enforce full cover or not.
   * @return
   */
  public static Long[] h3CellIDs(Geometry input, int level, boolean fullCover) {
    if (level < 0 || level > 15) {
      throw new IllegalArgumentException("level must be between 0 and 15");
    }
    HashSet<Long> cellIds = new HashSet<>();
    // first flat all GeometryCollection implementations into single Geometry types (Polygon,
    // LineString, Point)
    List<Geometry> geoms = GeomUtils.extractGeometryCollection(input);
    for (Geometry geom : geoms) {
      if (geom instanceof Polygon) {
        cellIds.addAll(H3Utils.polygonToCells((Polygon) geom, level, fullCover));
      } else if (geom instanceof LineString) {
        cellIds.addAll(H3Utils.lineStringToCells((LineString) geom, level, fullCover));
      } else if (geom instanceof Point) {
        cellIds.add(H3Utils.coordinateToCell(geom.getCoordinate(), level));
      } else {
        // if not type of polygon, point or lienSting, we cover its MBR
        cellIds.addAll(H3Utils.polygonToCells((Polygon) geom.getEnvelope(), level, fullCover));
      }
    }
    return cellIds.toArray(new Long[0]);
  }

  /**
   * return the distance between 2 cells, if the native h3 function doesn't work, use our
   * approximation function for shortest path and use the size - 1 as distance
   *
   * @param cell1 source cell
   * @param cell2 destination cell
   * @return
   */
  public static long h3CellDistance(long cell1, long cell2) {
    int resolution = H3Utils.h3.getResolution(cell1);
    if (resolution != H3Utils.h3.getResolution(cell2)) {
      throw new IllegalArgumentException("The argument cells should be of the same resolution");
    }
    try {
      return H3Utils.h3.gridDistance(cell1, cell2);
    } catch (H3Exception e) {
      // approximate if the original function hit error
      return H3Utils.approxPathCells(
                  H3Utils.cellToCoordinate(cell1),
                  H3Utils.cellToCoordinate(cell2),
                  resolution,
                  true)
              .size()
          - 1;
    }
  }

  /**
   * get the neighbor cells of the input cell by h3.gridDisk function
   *
   * @param cell: original cell
   * @param k: the k number of rings spread from the original cell
   * @param exactDistance: if exactDistance is true, it will only return the cells on the exact kth
   *     ring, else will return all 0 - kth neighbors
   * @return
   */
  public static Long[] h3KRing(long cell, int k, boolean exactDistance) {
    Set<Long> cells = new LinkedHashSet<>(H3Utils.h3.gridDisk(cell, k));
    if (exactDistance && k > 0) {
      List<Long> tbdCells = H3Utils.h3.gridDisk(cell, k - 1);
      tbdCells.forEach(cells::remove);
    }
    return cells.toArray(new Long[0]);
  }

  /**
   * gets the polygon for each h3 index provided
   *
   * @param cells: the set of cells
   * @return An Array of Polygons reversed
   */
  public static Geometry[] h3ToGeom(long[] cells) {
    GeometryFactory geomFactory = new GeometryFactory();
    List<Long> h3 = Arrays.stream(cells).boxed().collect(Collectors.toList());
    List<Polygon> polygons = new ArrayList<>();

    for (int j = 0; j < h3.size(); j++) {
      for (List<List<LatLng>> shellHoles :
          H3Utils.h3.cellsToMultiPolygon(Collections.singleton(h3.get(j)), true)) {
        List<LinearRing> rings = new ArrayList<>();
        for (List<LatLng> shell : shellHoles) {
          Coordinate[] coordinates = new Coordinate[shell.size()];
          for (int i = 0; i < shell.size(); i++) {
            LatLng latLng = shell.get(i);
            coordinates[i] = new Coordinate(latLng.lng, latLng.lat);
          }
          LinearRing ring = geomFactory.createLinearRing(coordinates);
          rings.add(ring);
        }

        LinearRing shell = rings.remove(0);
        if (rings.isEmpty()) {
          polygons.add(geomFactory.createPolygon(shell));
        } else {
          polygons.add(geomFactory.createPolygon(shell, rings.toArray(new LinearRing[0])));
        }
      }
    }
    return polygons.toArray(new Polygon[0]);
  }

  // create static function named simplifyPreserveTopology
  public static Geometry simplifyPreserveTopology(Geometry geometry, double distanceTolerance) {
    return TopologyPreservingSimplifier.simplify(geometry, distanceTolerance);
  }

  public static Geometry simplifyVW(Geometry geometry, double tolerance) {
    // JTS squares the tolerance in its implementation, inorder to keep it consistent square rooting
    // the input tolerance
    // source:
    // https://github.com/locationtech/jts/blob/7ef2b9d2e6f36ce5e7a787cff57bd18281e50826/modules/core/src/main/java/org/locationtech/jts/simplify/VWLineSimplifier.java#L41
    tolerance = Math.sqrt(tolerance);
    return VWSimplifier.simplify(geometry, tolerance);
  }

  public static Geometry simplifyPolygonHull(
      Geometry geometry, double vertexFactor, boolean isOuter) {
    return PolygonHullSimplifier.hull(geometry, isOuter, vertexFactor);
  }

  public static Geometry simplifyPolygonHull(Geometry geometry, double vertexFactor) {
    return simplifyPolygonHull(geometry, vertexFactor, true);
  }

  public static String geometryType(Geometry geometry) {
    return "ST_" + geometry.getGeometryType();
  }

  public static String geometryTypeWithMeasured(Geometry geometry) {
    String geometryType = geometry.getGeometryType().toUpperCase();
    if (GeomUtils.isMeasuredGeometry(geometry)) {
      geometryType += "M";
    }
    return geometryType;
  }

  public static Geometry startPoint(Geometry geometry) {
    if (geometry instanceof LineString) {
      LineString line = (LineString) geometry;
      return line.getStartPoint();
    }
    return null;
  }

  public static Geometry endPoint(Geometry geometry) {
    if (geometry instanceof LineString) {
      LineString line = (LineString) geometry;
      return line.getEndPoint();
    }
    return null;
  }

  public static Geometry[] dump(Geometry geometry) {
    int numGeom = geometry.getNumGeometries();
    if (geometry instanceof GeometryCollection) {
      Geometry[] geoms = new Geometry[geometry.getNumGeometries()];
      for (int i = 0; i < numGeom; i++) {
        geoms[i] = geometry.getGeometryN(i);
      }
      return geoms;
    } else {
      return new Geometry[] {geometry};
    }
  }

  public static Geometry[] dumpPoints(Geometry geometry) {
    GeometryFactory factory = geometry.getFactory();
    return Arrays.stream(geometry.getCoordinates()).map(factory::createPoint).toArray(Point[]::new);
  }

  public static Geometry symDifference(Geometry leftGeom, Geometry rightGeom) {
    return leftGeom.symDifference(rightGeom);
  }

  public static Geometry union(Geometry leftGeom, Geometry rightGeom) {
    return leftGeom.union(rightGeom);
  }

  public static Geometry union(Geometry[] geoms) {
    GeometryFactory factory = (geoms.length > 0) ? geoms[0].getFactory() : new GeometryFactory();
    return factory.createGeometryCollection(geoms).union();
  }

  public static Geometry unaryUnion(Geometry geom) {
    return UnaryUnionOp.union(geom);
  }

  public static Geometry createMultiGeometryFromOneElement(Geometry geometry) {
    GeometryFactory factory = geometry.getFactory();
    if (geometry instanceof Circle) {
      return factory.createGeometryCollection(new Circle[] {(Circle) geometry});
    } else if (geometry instanceof GeometryCollection) {
      return geometry;
    } else if (geometry instanceof LineString) {
      return factory.createMultiLineString(new LineString[] {(LineString) geometry});
    } else if (geometry instanceof Point) {
      return factory.createMultiPoint(new Point[] {(Point) geometry});
    } else if (geometry instanceof Polygon) {
      return factory.createMultiPolygon(new Polygon[] {(Polygon) geometry});
    } else {
      return factory.createGeometryCollection();
    }
  }

  public static Geometry[] subDivide(Geometry geometry, int maxVertices) {
    return GeometrySubDivider.subDivide(geometry, maxVertices);
  }

  public static Geometry snap(Geometry input, Geometry reference, double tolerance) {
    GeometrySnapper snapper = new GeometrySnapper(input);
    return snapper.snapTo(reference, tolerance);
  }

  public static Geometry makeLine(Geometry geom1, Geometry geom2) {
    Geometry[] geoms = new Geometry[] {geom1, geom2};
    return makeLine(geoms);
  }

  public static Geometry makeLine(Geometry[] geoms) {
    ArrayList<Coordinate> coordinates = new ArrayList<>();
    for (Geometry geom : geoms) {
      if (geom instanceof Point || geom instanceof MultiPoint || geom instanceof LineString) {
        for (Coordinate coord : geom.getCoordinates()) {
          coordinates.add(coord);
        }
      } else {
        throw new IllegalArgumentException(
            "ST_MakeLine only supports Point, MultiPoint and LineString geometries");
      }
    }

    Coordinate[] coords = coordinates.toArray(new Coordinate[0]);
    GeometryFactory factory = (geoms.length > 0) ? geoms[0].getFactory() : new GeometryFactory();
    return factory.createLineString(coords);
  }

  public static Geometry makePolygon(Geometry shell, Geometry[] holes) {
    GeometryFactory factory = shell.getFactory();
    return makePolygon(shell, holes, factory);
  }

  public static Geometry makePolygon(Geometry shell, Geometry[] holes, GeometryFactory factory) {
    try {
      if (holes != null) {
        LinearRing[] interiorRings =
            Arrays.stream(holes)
                .filter(
                    h ->
                        h != null
                            && !h.isEmpty()
                            && h instanceof LineString
                            && ((LineString) h).isClosed())
                .map(h -> factory.createLinearRing(h.getCoordinates()))
                .toArray(LinearRing[]::new);
        if (interiorRings.length != 0) {
          return factory.createPolygon(
              factory.createLinearRing(shell.getCoordinates()),
              Arrays.stream(holes)
                  .filter(
                      h ->
                          h != null
                              && !h.isEmpty()
                              && h instanceof LineString
                              && ((LineString) h).isClosed())
                  .map(h -> factory.createLinearRing(h.getCoordinates()))
                  .toArray(LinearRing[]::new));
        }
      }
      return factory.createPolygon(factory.createLinearRing(shell.getCoordinates()));
    } catch (IllegalArgumentException e) {
      return null;
    }
  }

  public static Geometry makepolygonWithSRID(Geometry lineString, Integer srid) {
    GeometryFactory factory =
        (srid != null) ? new GeometryFactory(new PrecisionModel(), srid) : lineString.getFactory();
    return makePolygon(lineString, null, factory);
  }

  public static Geometry createMultiGeometry(Geometry[] geometries) {
    if (geometries.length > 1) {
      return geometries[0].getFactory().buildGeometry(Arrays.asList(geometries));
    } else if (geometries.length == 1) {
      return createMultiGeometryFromOneElement(geometries[0]);
    } else {
      GeometryFactory factory = new GeometryFactory();
      return factory.createGeometryCollection();
    }
  }

  public static Geometry collectionExtract(Geometry geometry, Integer geomType) {
    if (geomType == null) {
      return collectionExtract(geometry);
    }
    GeometryFactory factory = geometry.getFactory();
    Class<? extends Geometry> geomClass;
    GeometryCollection emptyResult;
    switch (geomType) {
      case 1:
        geomClass = Point.class;
        emptyResult = factory.createMultiPoint();
        break;
      case 2:
        geomClass = LineString.class;
        emptyResult = factory.createMultiLineString();
        break;
      case 3:
        geomClass = Polygon.class;
        emptyResult = factory.createMultiPolygon();
        break;
      default:
        throw new IllegalArgumentException("Invalid geometry type");
    }
    List<Geometry> geometries = GeomUtils.extractGeometryCollection(geometry, geomClass);
    if (geometries.isEmpty()) {
      return emptyResult;
    }
    return Functions.createMultiGeometry(geometries.toArray(new Geometry[0]));
  }

  public static Geometry collectionExtract(Geometry geometry) {
    List<Geometry> geometries = GeomUtils.extractGeometryCollection(geometry);
    GeometryFactory factory = geometry.getFactory();
    Polygon[] polygons =
        geometries.stream().filter(g -> g instanceof Polygon).toArray(Polygon[]::new);
    if (polygons.length > 0) {
      return factory.createMultiPolygon(polygons);
    }
    LineString[] lines =
        geometries.stream().filter(g -> g instanceof LineString).toArray(LineString[]::new);
    if (lines.length > 0) {
      return factory.createMultiLineString(lines);
    }
    Point[] points = geometries.stream().filter(g -> g instanceof Point).toArray(Point[]::new);
    if (points.length > 0) {
      return factory.createMultiPoint(points);
    }
    return factory.createGeometryCollection();
  }

  // ported from
  // https://github.com/postgis/postgis/blob/f6ed58d1fdc865d55d348212d02c11a10aeb2b30/liblwgeom/lwgeom_median.c
  // geometry ST_GeometricMedian ( geometry g , float8 tolerance , int max_iter , boolean
  // fail_if_not_converged );

  private static double distance3d(Coordinate p1, Coordinate p2) {
    double dx = p2.x - p1.x;
    double dy = p2.y - p1.y;
    double dz = p2.z - p1.z;
    return Math.sqrt(dx * dx + dy * dy + dz * dz);
  }

  private static void distances(Coordinate curr, Coordinate[] points, double[] distances) {
    for (int i = 0; i < points.length; i++) {
      distances[i] = distance3d(curr, points[i]);
    }
  }

  private static double iteratePoints(Coordinate curr, Coordinate[] points, double[] distances) {
    Coordinate next = new Coordinate(0, 0, 0);
    double delta = 0;
    double denom = 0;
    boolean hit = false;
    distances(curr, points, distances);

    for (int i = 0; i < points.length; i++) {
      /* we need to use lower epsilon than in FP_IS_ZERO in the loop for calculation to converge */
      double distance = distances[i];
      if (distance > DBL_EPSILON) {
        Coordinate coordinate = points[i];
        next.x += coordinate.x / distance;
        next.y += coordinate.y / distance;
        next.z += coordinate.z / distance;
        denom += 1.0 / distance;
      } else {
        hit = true;
      }
    }
    /* negative weight shouldn't get here */
    // assert(denom >= 0);

    /* denom is zero in case of multipoint of single point when we've converged perfectly */
    if (denom > DBL_EPSILON) {
      next.x /= denom;
      next.y /= denom;
      next.z /= denom;

      /* If any of the intermediate points in the calculation is found in the
       * set of input points, the standard Weiszfeld method gets stuck with a
       * divide-by-zero.
       *
       * To get ourselves out of the hole, we follow an alternate procedure to
       * get the next iteration, as described in:
       *
       * Vardi, Y. and Zhang, C. (2011) "A modified Weiszfeld algorithm for the
       * Fermat-Weber location problem."  Math. Program., Ser. A 90: 559-566.
       * DOI 10.1007/s101070100222
       *
       * Available online at the time of this writing at
       * http://www.stat.rutgers.edu/home/cunhui/papers/43.pdf
       */
      if (hit) {
        double dx = 0;
        double dy = 0;
        double dz = 0;
        for (int i = 0; i < points.length; i++) {
          double distance = distances[i];
          if (distance > DBL_EPSILON) {
            Coordinate coordinate = points[i];
            dx += (coordinate.x - curr.x) / distance;
            dy += (coordinate.y - curr.y) / distance;
            dz += (coordinate.z - curr.z) / distance;
          }
        }
        double dSqr = Math.sqrt(dx * dx + dy * dy + dz * dz);
        /* Avoid division by zero if the intermediate point is the median */
        if (dSqr > DBL_EPSILON) {
          double rInv = Math.max(0, 1.0 / dSqr);
          next.x = (1.0 - rInv) * next.x + rInv * curr.x;
          next.y = (1.0 - rInv) * next.y + rInv * curr.y;
          next.z = (1.0 - rInv) * next.z + rInv * curr.z;
        }
      }
      delta = distance3d(curr, next);
      curr.x = next.x;
      curr.y = next.y;
      curr.z = next.z;
    }
    return delta;
  }

  private static Coordinate initGuess(Coordinate[] points) {
    Coordinate guess = new Coordinate(0, 0, 0);
    for (Coordinate point : points) {
      guess.x += point.x / points.length;
      guess.y += point.y / points.length;
      guess.z += point.z / points.length;
    }
    return guess;
  }

  private static Coordinate[] extractCoordinates(Geometry geometry) {
    Coordinate[] points = geometry.getCoordinates();
    if (points.length == 0) return points;

    Coordinate[] coordinates = new Coordinate[points.length];
    for (int i = 0; i < points.length; i++) {
      boolean is3d = !Double.isNaN(points[i].z);
      coordinates[i] = points[i].copy();
      if (!is3d) coordinates[i].z = 0.0;
    }
    return coordinates;
  }

  public static int numPoints(Geometry geometry) throws Exception {
    String geometryType = geometry.getGeometryType();
    if (!(Geometry.TYPENAME_LINESTRING.equalsIgnoreCase(geometryType))) {
      throw new IllegalArgumentException(
          "Unsupported geometry type: "
              + geometryType
              + ", only LineString geometry is supported.");
    }
    return geometry.getNumPoints();
  }

  public static Geometry force3DM(Geometry geom, double mValue) {
    return GeometryForce3DMTransformer.transform(geom, mValue);
  }

  public static Geometry force3DM(Geometry geom) {
    return force3DM(geom, 0.0);
  }

  public static Geometry force4D(Geometry geom, double zValue, double mValue) {
    return GeometryForce4DTransformer.transform(geom, zValue, mValue);
  }

  public static Geometry force4D(Geometry geom) {
    return force4D(geom, 0.0, 0.0);
  }

  public static Geometry force3D(Geometry geometry, double zValue) {
    return GeometryForce3DTransformer.transform(geometry, zValue);
  }

  public static Geometry force3D(Geometry geometry) {
    return GeometryForce3DTransformer.transform(geometry, 0.0);
  }

  public static Geometry forceCollection(Geometry geom) {
    return geom.getFactory().createGeometryCollection(convertGeometryToArray(geom));
  }

  private static Geometry[] convertGeometryToArray(Geometry geom) {
    Geometry[] array = new Geometry[geom.getNumGeometries()];
    for (int i = 0; i < array.length; i++) {
      array[i] = geom.getGeometryN(i);
    }
    return array;
  }

  public static Geometry generatePoints(Geometry geom, int numPoints, long seed) {
    RandomPointsBuilderSeed pointsBuilder = new RandomPointsBuilderSeed(geom.getFactory(), seed);
    pointsBuilder.setExtent(geom);
    pointsBuilder.setNumPoints(numPoints);
    return pointsBuilder.getGeometry();
  }

  public static Geometry generatePoints(Geometry geom, int numPoints) {
    return generatePoints(geom, numPoints, 0);
  }

  public static Geometry generatePoints(Geometry geom, int numPoints, Random random) {
    RandomPointsBuilderSeed pointsBuilder = new RandomPointsBuilderSeed(geom.getFactory(), random);
    pointsBuilder.setExtent(geom);
    pointsBuilder.setNumPoints(numPoints);
    return pointsBuilder.getGeometry();
  }

  public static Integer nRings(Geometry geometry) throws Exception {
    String geometryType = geometry.getGeometryType();
    if (!(geometry instanceof Polygon || geometry instanceof MultiPolygon)) {
      throw new IllegalArgumentException(
          "Unsupported geometry type: "
              + geometryType
              + ", only Polygon or MultiPolygon geometries are supported.");
    }
    int numRings = 0;
    if (geometry instanceof Polygon) {
      Polygon polygon = (Polygon) geometry;
      numRings = GeomUtils.getPolygonNumRings(polygon);
    } else {
      MultiPolygon multiPolygon = (MultiPolygon) geometry;
      int numPolygons = multiPolygon.getNumGeometries();
      for (int i = 0; i < numPolygons; i++) {
        Polygon polygon = (Polygon) multiPolygon.getGeometryN(i);
        numRings += GeomUtils.getPolygonNumRings(polygon);
      }
    }
    return numRings;
  }

  public static Geometry translate(Geometry geometry, double deltaX, double deltaY, double deltaZ) {
    if (!geometry.isEmpty()) {
      GeomUtils.translateGeom(geometry, deltaX, deltaY, deltaZ);
    }
    return geometry;
  }

  public static Geometry translate(Geometry geometry, double deltaX, double deltaY) {
    if (!geometry.isEmpty()) {
      GeomUtils.translateGeom(geometry, deltaX, deltaY, 0.0);
    }
    return geometry;
  }

  public static Geometry affine(
      Geometry geometry,
      double a,
      double b,
      double c,
      double d,
      double e,
      double f,
      double g,
      double h,
      double i,
      double xOff,
      double yOff,
      double zOff) {
    if (!geometry.isEmpty()) {
      GeomUtils.affineGeom(geometry, a, b, c, d, e, f, g, h, i, xOff, yOff, zOff);
    }
    return geometry;
  }

  public static Geometry affine(
      Geometry geometry, double a, double b, double d, double e, double xOff, double yOff) {
    if (!geometry.isEmpty()) {
      GeomUtils.affineGeom(geometry, a, b, null, d, e, null, null, null, null, xOff, yOff, null);
    }
    return geometry;
  }

  public static Geometry geometricMedian(
      Geometry geometry, double tolerance, int maxIter, boolean failIfNotConverged)
      throws Exception {
    GeometryFactory factory = geometry.getFactory();
    String geometryType = geometry.getGeometryType();
    if (!(Geometry.TYPENAME_POINT.equals(geometryType)
        || Geometry.TYPENAME_MULTIPOINT.equals(geometryType))) {
      throw new Exception("Unsupported geometry type: " + geometryType);
    }
    Coordinate[] coordinates = extractCoordinates(geometry);
    if (coordinates.length == 0) return new Point(null, factory);
    Coordinate median = initGuess(coordinates);
    double delta = Double.MAX_VALUE;
    double[] distances =
        new double[coordinates.length]; // preallocate to reduce gc pressure for large iterations
    for (int i = 0; i < maxIter && delta > tolerance; i++)
      delta = iteratePoints(median, coordinates, distances);
    if (failIfNotConverged && delta > tolerance)
      throw new Exception(
          String.format(
              "Median failed to converge within %.1E after %d iterations.", tolerance, maxIter));
    boolean is3d = !Double.isNaN(geometry.getCoordinate().z);
    if (!is3d) median.z = Double.NaN;
    return factory.createPoint(median);
  }

  public static Geometry geometricMedian(Geometry geometry, double tolerance, int maxIter)
      throws Exception {
    return geometricMedian(geometry, tolerance, maxIter, false);
  }

  public static Geometry geometricMedian(Geometry geometry, double tolerance) throws Exception {
    return geometricMedian(geometry, tolerance, DEFAULT_MAX_ITER, false);
  }

  public static Geometry geometricMedian(Geometry geometry) throws Exception {
    return geometricMedian(geometry, DEFAULT_TOLERANCE, DEFAULT_MAX_ITER, false);
  }

  public static double frechetDistance(Geometry g1, Geometry g2) {
    return GeomUtils.getFrechetDistance(g1, g2);
  }

  public static boolean isCollection(Geometry geometry) {
    String geoType = geometry.getGeometryType();
    return Geometry.TYPENAME_GEOMETRYCOLLECTION.equalsIgnoreCase(geoType)
        || Geometry.TYPENAME_MULTIPOINT.equalsIgnoreCase(geoType)
        || Geometry.TYPENAME_MULTIPOLYGON.equalsIgnoreCase(geoType)
        || Geometry.TYPENAME_MULTILINESTRING.equalsIgnoreCase(geoType);
  }

  public static Geometry boundingDiagonal(Geometry geometry) {
    GeometryFactory factory = geometry.getFactory();
    if (geometry.isEmpty()) {
      return factory.createLineString();
    } else {
      Double startX = null, startY = null, startZ = null, endX = null, endY = null, endZ = null;
      boolean is3d = !Double.isNaN(geometry.getCoordinate().z);
      for (Coordinate currCoordinate : geometry.getCoordinates()) {
        startX = startX == null ? currCoordinate.getX() : Math.min(startX, currCoordinate.getX());
        startY = startY == null ? currCoordinate.getY() : Math.min(startY, currCoordinate.getY());

        endX = endX == null ? currCoordinate.getX() : Math.max(endX, currCoordinate.getX());
        endY = endY == null ? currCoordinate.getY() : Math.max(endY, currCoordinate.getY());
        if (is3d) {
          Double geomZ = currCoordinate.getZ();
          startZ = startZ == null ? currCoordinate.getZ() : Math.min(startZ, currCoordinate.getZ());
          endZ = endZ == null ? currCoordinate.getZ() : Math.max(endZ, currCoordinate.getZ());
        }
      }
      Coordinate startCoordinate;
      Coordinate endCoordinate;
      if (is3d) {
        startCoordinate = new Coordinate(startX, startY, startZ);
        endCoordinate = new Coordinate(endX, endY, endZ);
      } else {
        startCoordinate = new Coordinate(startX, startY);
        endCoordinate = new Coordinate(endX, endY);
      }
      return factory.createLineString(new Coordinate[] {startCoordinate, endCoordinate});
    }
  }

  public static double angle(Geometry point1, Geometry point2, Geometry point3, Geometry point4)
      throws IllegalArgumentException {
    if (point3 == null && point4 == null) return Functions.angle(point1, point2);
    else if (point4 == null) return Functions.angle(point1, point2, point3);
    if (GeomUtils.isAnyGeomEmpty(point1, point2, point3, point4))
      throw new IllegalArgumentException("ST_Angle cannot support empty geometries.");
    if (!(point1 instanceof Point
        && point2 instanceof Point
        && point3 instanceof Point
        && point4 instanceof Point))
      throw new IllegalArgumentException(
          "ST_Angle supports either only POINT or only LINESTRING geometries.");
    return GeomUtils.calcAngle(
        point1.getCoordinate(),
        point2.getCoordinate(),
        point3.getCoordinate(),
        point4.getCoordinate());
  }

  public static double angle(Geometry point1, Geometry point2, Geometry point3)
      throws IllegalArgumentException {
    if (GeomUtils.isAnyGeomEmpty(point1, point2, point3))
      throw new IllegalArgumentException("ST_Angle cannot support empty geometries.");
    if (!(point1 instanceof Point && point2 instanceof Point && point3 instanceof Point))
      throw new IllegalArgumentException(
          "ST_Angle supports either only POINT or only LINESTRING geometries.");
    return GeomUtils.calcAngle(
        point2.getCoordinate(),
        point1.getCoordinate(),
        point2.getCoordinate(),
        point3.getCoordinate());
  }

  public static double angle(Geometry line1, Geometry line2) throws IllegalArgumentException {
    if (GeomUtils.isAnyGeomEmpty(line1, line2))
      throw new IllegalArgumentException("ST_Angle cannot support empty geometries.");
    if (!(line1 instanceof LineString && line2 instanceof LineString))
      throw new IllegalArgumentException(
          "ST_Angle supports either only POINT or only LINESTRING geometries.");
    Coordinate[] startEndLine1 = GeomUtils.getStartEndCoordinates(line1);
    Coordinate[] startEndLine2 = GeomUtils.getStartEndCoordinates(line2);
    assert startEndLine1 != null;
    assert startEndLine2 != null;
    return GeomUtils.calcAngle(
        startEndLine1[0], startEndLine1[1], startEndLine2[0], startEndLine2[1]);
  }

  public static double degrees(double angleInRadian) {
    return GeomUtils.toDegrees(angleInRadian);
  }

  public static Double hausdorffDistance(Geometry g1, Geometry g2, double densityFrac) {
    return GeomUtils.getHausdorffDistance(g1, g2, densityFrac);
  }

  public static Double hausdorffDistance(Geometry g1, Geometry g2) {
    return GeomUtils.getHausdorffDistance(g1, g2, -1);
  }

  private static IsValidOp getIsValidOpObject(Geometry geom, int flag) {
    IsValidOp isValidOp = new IsValidOp(geom);

    // Set the validity model based on flags
    if (flag == ESRI_VALIDITY) {
      isValidOp.setSelfTouchingRingFormingHoleValid(true);
    } else {
      isValidOp.setSelfTouchingRingFormingHoleValid(false);
    }
    return isValidOp;
  }

  public static ValidDetail isValidDetail(Geometry geom) {
    return isValidDetail(geom, OGC_SFS_VALIDITY);
  }

  public static ValidDetail isValidDetail(Geometry geom, int flag) {
    IsValidOp isValidOp = getIsValidOpObject(geom, flag);

    if (isValidOp.isValid()) {
      return new ValidDetail(true, null, null);
    } else {
      TopologyValidationError error = isValidOp.getValidationError();
      String reason = error.toString();
      Geometry location = geom.getFactory().createPoint(error.getCoordinate());
      return new ValidDetail(false, reason, location);
    }
  }

  public static String isValidReason(Geometry geom) {
    return isValidReason(geom, OGC_SFS_VALIDITY);
  }

  public static String isValidReason(Geometry geom, int flag) {
    IsValidOp isValidOp = getIsValidOpObject(geom, flag);

    if (isValidOp.isValid()) {
      return "Valid Geometry";
    } else {
      TopologyValidationError error = isValidOp.getValidationError();
      return error.toString();
    }
  }

  /**
   * This method takes a GeometryCollection and returns another GeometryCollection containing the
   * polygons formed by the linework of a set of geometries.
   *
   * @param geometry A collection of Geometry objects that should contain only linestrings.
   * @return A GeometryCollection containing the resultant polygons.
   */
  public static Geometry polygonize(Geometry geometry) {
    GeometryFactory factory = (geometry != null) ? geometry.getFactory() : new GeometryFactory();
    if (geometry == null || geometry.isEmpty()) {
      return factory.createGeometryCollection(null);
    }

    if (geometry instanceof GeometryCollection) {
      Polygonizer polygonizer = new Polygonizer();

      for (int i = 0; i < geometry.getNumGeometries(); i++) {
        polygonizer.add(geometry.getGeometryN(i));
      }

      Collection polygons = polygonizer.getPolygons();
      Geometry[] polyArray = (Geometry[]) polygons.toArray(new Geometry[0]);

      return factory.createGeometryCollection(polyArray);
    } else {
      return factory.createGeometryCollection(null);
    }
  }

  /**
   * Creates a MultiPoint containing all coordinates of the given geometry. Duplicates, M and Z
   * coordinates are preserved.
   *
   * @param geometry The input geometry
   * @return A MultiPoint geometry
   */
  public static Geometry points(Geometry geometry) {
    if (geometry == null) {
      return null;
    }

    // Extracting all coordinates from the geometry
    Coordinate[] coordinates = geometry.getCoordinates();

    // Creating a MultiPoint from the extracted coordinates
    return geometry.getFactory().createMultiPointFromCoords(coordinates);
  }

  public static Geometry rotateX(Geometry geometry, double angle) {
    if (GeomUtils.isAnyGeomEmpty(geometry)) {
      return geometry;
    }
    double sinAngle = Math.sin(angle);
    double cosAngle = Math.cos(angle);
    return affine(geometry, 1, 0, 0, 0, cosAngle, -sinAngle, 0, sinAngle, cosAngle, 0, 0, 0);
  }

  /**
   * Rotates a geometry by a given angle in radians.
   *
   * @param geometry The input geometry to rotate.
   * @param angle The angle in radians to rotate the geometry.
   * @return The rotated geometry.
   */
  public static Geometry rotate(Geometry geometry, double angle) {
    if (geometry == null || geometry.isEmpty()) {
      return geometry;
    }
    AffineTransformation rotation = AffineTransformation.rotationInstance(angle);
    return rotation.transform(geometry);
  }

  /**
   * Rotates a geometry by a given angle in radians around a given origin point (x, y).
   *
   * @param geometry The input geometry to rotate.
   * @param angle The angle in radians to rotate the geometry.
   * @param originX The x coordinate of the origin point around which to rotate.
   * @param originY The y coordinate of the origin point around which to rotate.
   * @return The rotated geometry.
   */
  public static Geometry rotate(Geometry geometry, double angle, double originX, double originY) {
    if (geometry == null || geometry.isEmpty()) {
      return geometry;
    }
    AffineTransformation rotation = AffineTransformation.rotationInstance(angle, originX, originY);
    return rotation.transform(geometry);
  }

  /**
   * Rotates a geometry by a given angle in radians around a given origin point.
   *
   * @param geometry The input geometry to rotate.
   * @param angle The angle in radians to rotate the geometry.
   * @param pointOrigin The origin point around which to rotate.
   * @return The rotated geometry.
   * @throws IllegalArgumentException if the pointOrigin is not a Point geometry.
   */
  public static Geometry rotate(Geometry geometry, double angle, Geometry pointOrigin) {
    if (geometry == null || geometry.isEmpty()) {
      return geometry;
    }
    if (pointOrigin == null || pointOrigin.isEmpty() || !(pointOrigin instanceof Point)) {
      throw new IllegalArgumentException("The origin must be a non-empty Point geometry.");
    }
    Point origin = (Point) pointOrigin;
    double originX = origin.getX();
    double originY = origin.getY();
    AffineTransformation rotation = AffineTransformation.rotationInstance(angle, originX, originY);
    return rotation.transform(geometry);
  }
}
