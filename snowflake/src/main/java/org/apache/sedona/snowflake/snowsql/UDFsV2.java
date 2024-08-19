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

import java.io.IOException;
import org.apache.sedona.common.Functions;
import org.apache.sedona.common.FunctionsGeoTools;
import org.apache.sedona.common.Predicates;
import org.apache.sedona.common.sphere.Haversine;
import org.apache.sedona.common.sphere.Spheroid;
import org.apache.sedona.snowflake.snowsql.annotations.UDFAnnotations;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;

/**
 * User defined functions for Apache Sedona This class is used to generate DDL for Snowflake UDFs
 * Different from UDFs.java, this class only contains functions can directly interact with Snowflake
 * native functions. This means no Constructors (ST_GeomFromXXX). Technically any ST functions that
 * do not take geometry as input are ignored here. The trick here is to overload the functions in
 * UDFs.java and UDFsV2.java. This requires all functions must take a GeoJSON string as input and
 * return a GeoJSON string as output.
 */
public class UDFsV2 {
  @UDFAnnotations.ParamMeta(
      argNames = {"geometry"},
      argTypes = {"Geometry"})
  public static String GeometryType(String geometry) {
    return Functions.geometryTypeWithMeasured(GeometrySerde.deserGeoJson(geometry));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"linestring", "point", "position"},
      argTypes = {"Geometry", "Geometry", "int"},
      returnTypes = "Geometry")
  public static String ST_AddPoint(String linestring, String point, int position) {
    return GeometrySerde.serGeoJson(
        Functions.addPoint(
            GeometrySerde.deserGeoJson(linestring), GeometrySerde.deserGeoJson(point), position));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geometry", "a", "b", "c", "d", "e", "f", "g", "h", "i", "xOff", "yOff", "zOff"},
      argTypes = {
        "Geometry",
        "double",
        "double",
        "double",
        "double",
        "double",
        "double",
        "double",
        "double",
        "double",
        "double",
        "double",
        "double"
      },
      returnTypes = "Geometry")
  public static String ST_Affine(
      String geometry,
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
    return GeometrySerde.serGeoJson(
        Functions.affine(
            GeometrySerde.deserGeoJson(geometry), a, b, c, d, e, f, g, h, i, xOff, yOff, zOff));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geometry", "a", "b", "c", "d", "e", "f", "xOff", "yOff"},
      argTypes = {"Geometry", "double", "double", "double", "double", "double", "double"},
      returnTypes = "Geometry")
  public static String ST_Affine(
      String geometry, double a, double b, double d, double e, double xOff, double yOff) {
    return GeometrySerde.serGeoJson(
        Functions.affine(GeometrySerde.deserGeoJson(geometry), a, b, d, e, xOff, yOff));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geom1", "geom2"},
      argTypes = {"Geometry", "Geometry"})
  public static double ST_Angle(String geom1, String geom2) {
    return Functions.angle(GeometrySerde.deserGeoJson(geom1), GeometrySerde.deserGeoJson(geom2));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geom1", "geom2", "geom3"},
      argTypes = {"Geometry", "Geometry", "Geometry"})
  public static double ST_Angle(String geom1, String geom2, String geom3) {
    return Functions.angle(
        GeometrySerde.deserGeoJson(geom1),
        GeometrySerde.deserGeoJson(geom2),
        GeometrySerde.deserGeoJson(geom3));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geom1", "geom2", "geom3", "geom4"},
      argTypes = {"Geometry", "Geometry", "Geometry", "Geometry"})
  public static double ST_Angle(String geom1, String geom2, String geom3, String geom4) {
    return Functions.angle(
        GeometrySerde.deserGeoJson(geom1),
        GeometrySerde.deserGeoJson(geom2),
        GeometrySerde.deserGeoJson(geom3),
        GeometrySerde.deserGeoJson(geom4));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geometry"},
      argTypes = {"Geometry"})
  public static double ST_Area(String geometry) {
    return Functions.area(GeometrySerde.deserGeoJson(geometry));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geometry"},
      argTypes = {"Geometry"})
  public static byte[] ST_AsBinary(String geometry) {
    return Functions.asWKB(GeometrySerde.deserGeoJson(geometry));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geometry"},
      argTypes = {"Geometry"})
  public static byte[] ST_AsEWKB(String geometry) {
    return Functions.asEWKB(GeometrySerde.deserGeoJson(geometry));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geometry"},
      argTypes = {"Geometry"})
  public static String ST_AsHEXEWKB(String geometry) {
    return Functions.asHexEWKB(GeometrySerde.deserGeoJson(geometry));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geometry", "endian"},
      argTypes = {"Geometry", "String"})
  public static String ST_AsHEXEWKB(String geometry, String endian) {
    return Functions.asHexEWKB(GeometrySerde.deserGeoJson(geometry), endian);
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geometry"},
      argTypes = {"Geometry"})
  public static String ST_AsEWKT(String geometry) {
    return Functions.asEWKT(GeometrySerde.deserGeoJson(geometry));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geometry"},
      argTypes = {"Geometry"})
  public static String ST_AsGML(String geometry) {
    return Functions.asGML(GeometrySerde.deserGeoJson(geometry));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geometry"},
      argTypes = {"Geometry"})
  public static String ST_AsGeoJSON(String geometry) {
    return Functions.asGeoJson(GeometrySerde.deserGeoJson(geometry));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geometry", "type"},
      argTypes = {"Geometry", "String"})
  public static String ST_AsGeoJSON(String geometry, String type) {
    return Functions.asGeoJson(GeometrySerde.deserGeoJson(geometry), type);
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geometry"},
      argTypes = {"Geometry"})
  public static String ST_AsKML(String geometry) {
    return Functions.asKML(GeometrySerde.deserGeoJson(geometry));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"left", "right"},
      argTypes = {"Geometry", "Geometry"})
  public static double ST_Azimuth(String left, String right) {
    return Functions.azimuth(GeometrySerde.deserGeoJson(left), GeometrySerde.deserGeoJson(right));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geometry"},
      argTypes = {"Geometry"},
      returnTypes = "Geometry")
  public static String ST_Boundary(String geometry) {
    return GeometrySerde.serGeoJson(Functions.boundary(GeometrySerde.deserGeoJson(geometry)));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geometry"},
      argTypes = {"Geometry"},
      returnTypes = "Geometry")
  public static String ST_BoundingDiagonal(String geometry) {
    return GeometrySerde.serGeoJson(
        Functions.boundingDiagonal(GeometrySerde.deserGeoJson(geometry)));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geometry"},
      argTypes = {"Geometry"})
  public static int ST_BestSRID(String geometry) {
    return Functions.bestSRID(GeometrySerde.deserGeoJson(geometry));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geometry"},
      argTypes = {"Geometry"})
  public static String ST_ShiftLongitude(String geometry) {
    return GeometrySerde.serGeoJson(Functions.shiftLongitude(GeometrySerde.deserGeoJson(geometry)));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geometry", "radius"},
      argTypes = {"Geometry", "double"},
      returnTypes = "Geometry")
  public static String ST_Buffer(String geometry, double radius) throws IllegalArgumentException {
    return GeometrySerde.serGeoJson(Functions.buffer(GeometrySerde.deserGeoJson(geometry), radius));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geometry", "radius", "useSpheroid"},
      argTypes = {"Geometry", "double", "boolean"},
      returnTypes = "Geometry")
  public static String ST_Buffer(String geometry, double radius, boolean useSpheroid)
      throws IllegalArgumentException {
    return GeometrySerde.serGeoJson(
        Functions.buffer(GeometrySerde.deserGeoJson(geometry), radius, useSpheroid));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geometry", "radius", "useSpheroid", "parameters"},
      argTypes = {"Geometry", "double", "boolean", "String"},
      returnTypes = "Geometry")
  public static String ST_Buffer(
      String geometry, double radius, boolean useSpheroid, String parameters)
      throws IllegalArgumentException {
    return GeometrySerde.serGeoJson(
        Functions.buffer(GeometrySerde.deserGeoJson(geometry), radius, useSpheroid, parameters));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geometry"},
      argTypes = {"Geometry"},
      returnTypes = "Geometry")
  public static String ST_BuildArea(String geometry) {
    return GeometrySerde.serGeoJson(Functions.buildArea(GeometrySerde.deserGeoJson(geometry)));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geometry"},
      argTypes = {"Geometry"},
      returnTypes = "Geometry")
  public static String ST_Centroid(String geometry) {
    return GeometrySerde.serGeoJson(Functions.getCentroid(GeometrySerde.deserGeoJson(geometry)));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geometry1", "geometry2"},
      argTypes = {"Geometry", "Geometry"},
      returnTypes = "Geometry")
  public static String ST_ClosestPoint(String geom1, String geom2) {
    return GeometrySerde.serGeoJson(
        Functions.closestPoint(
            GeometrySerde.deserGeoJson(geom1), GeometrySerde.deserGeoJson(geom2)));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geometry"},
      argTypes = {"Geometry"},
      returnTypes = "Geometry")
  public static String ST_CollectionExtract(String geometry) throws IOException {
    return GeometrySerde.serGeoJson(
        Functions.collectionExtract(GeometrySerde.deserGeoJson(geometry)));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geometry", "geomType"},
      argTypes = {"Geometry", "int"},
      returnTypes = "Geometry")
  public static String ST_CollectionExtract(String geometry, int geomType) throws IOException {
    return GeometrySerde.serGeoJson(
        Functions.collectionExtract(GeometrySerde.deserGeoJson(geometry), geomType));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geometry", "pctConvex"},
      argTypes = {"Geometry", "double"},
      returnTypes = "Geometry")
  public static String ST_ConcaveHull(String geometry, double pctConvex) {
    return GeometrySerde.serGeoJson(
        Functions.concaveHull(GeometrySerde.deserGeoJson(geometry), pctConvex, false));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geometry", "pctConvex", "allowHoles"},
      argTypes = {"Geometry", "double", "boolean"},
      returnTypes = "Geometry")
  public static String ST_ConcaveHull(String geometry, double pctConvex, boolean allowHoles) {
    return GeometrySerde.serGeoJson(
        Functions.concaveHull(GeometrySerde.deserGeoJson(geometry), pctConvex, allowHoles));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"leftGeometry", "rightGeometry"},
      argTypes = {"Geometry", "Geometry"})
  public static boolean ST_Contains(String leftGeometry, String rightGeometry) {
    return Predicates.contains(
        GeometrySerde.deserGeoJson(leftGeometry), GeometrySerde.deserGeoJson(rightGeometry));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geometry"},
      argTypes = {"Geometry"})
  public static int ST_CoordDim(String geometry) {
    return Functions.nDims(GeometrySerde.deserGeoJson(geometry));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geometry"},
      argTypes = {"Geometry"},
      returnTypes = "Geometry")
  public static String ST_ConvexHull(String geometry) {
    return GeometrySerde.serGeoJson(Functions.convexHull(GeometrySerde.deserGeoJson(geometry)));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"leftGeometry", "rightGeometry"},
      argTypes = {"Geometry", "Geometry"})
  public static boolean ST_CoveredBy(String leftGeometry, String rightGeometry) {
    return Predicates.coveredBy(
        GeometrySerde.deserGeoJson(leftGeometry), GeometrySerde.deserGeoJson(rightGeometry));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"leftGeometry", "rightGeometry"},
      argTypes = {"Geometry", "Geometry"})
  public static boolean ST_Covers(String leftGeometry, String rightGeometry) {
    return Predicates.covers(
        GeometrySerde.deserGeoJson(leftGeometry), GeometrySerde.deserGeoJson(rightGeometry));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"leftGeometry", "rightGeometry"},
      argTypes = {"Geometry", "Geometry"})
  public static boolean ST_Crosses(String leftGeometry, String rightGeometry) {
    return Predicates.crosses(
        GeometrySerde.deserGeoJson(leftGeometry), GeometrySerde.deserGeoJson(rightGeometry));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geometry"},
      argTypes = {"Geometry"})
  public static boolean ST_CrossesDateLine(String geometry) {
    return Functions.crossesDateLine(GeometrySerde.deserGeoJson(geometry));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geometry"},
      argTypes = {"Geometry"},
      returnTypes = "Geometry")
  public static String ST_DelaunayTriangles(String geometry) {
    return GeometrySerde.serGeoJson(
        Functions.delaunayTriangle(GeometrySerde.deserGeoJson(geometry)));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geometry", "tolerance"},
      argTypes = {"Geometry", "double"},
      returnTypes = "Geometry")
  public static String ST_DelaunayTriangles(String geometry, double tolerance) {
    return GeometrySerde.serGeoJson(
        Functions.delaunayTriangle(GeometrySerde.deserGeoJson(geometry), tolerance));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geometry", "tolerance", "flag"},
      argTypes = {"Geometry", "double", "int"},
      returnTypes = "Geometry")
  public static String ST_DelaunayTriangles(String geometry, double tolerance, int flag) {
    return GeometrySerde.serGeoJson(
        Functions.delaunayTriangle(GeometrySerde.deserGeoJson(geometry), tolerance, flag));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"leftGeometry", "rightGeometry"},
      argTypes = {"Geometry", "Geometry"},
      returnTypes = "Geometry")
  public static String ST_Difference(String leftGeometry, String rightGeometry) {
    return GeometrySerde.serGeoJson(
        Functions.difference(
            GeometrySerde.deserGeoJson(leftGeometry), GeometrySerde.deserGeoJson(rightGeometry)));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geometry"},
      argTypes = {"Geometry"})
  public static Integer ST_Dimension(String geometry) {
    return Functions.dimension(GeometrySerde.deserGeoJson(geometry));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"leftGeometry", "rightGeometry"},
      argTypes = {"Geometry", "Geometry"})
  public static boolean ST_Disjoint(String leftGeometry, String rightGeometry) {
    return Predicates.disjoint(
        GeometrySerde.deserGeoJson(leftGeometry), GeometrySerde.deserGeoJson(rightGeometry));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"left", "right"},
      argTypes = {"Geometry", "Geometry"})
  public static double ST_Distance(String left, String right) {
    return Functions.distance(GeometrySerde.deserGeoJson(left), GeometrySerde.deserGeoJson(right));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"left", "right"},
      argTypes = {"Geometry", "Geometry"})
  public static double ST_3DDistance(String left, String right) {
    return Functions.distance3d(
        GeometrySerde.deserGeoJson(left), GeometrySerde.deserGeoJson(right));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geometry"},
      argTypes = {"Geometry"},
      returnTypes = "Geometry")
  public static String ST_DumpPoints(String geometry) {
    Geometry[] points = Functions.dumpPoints(GeometrySerde.deserGeoJson(geometry));
    return GeometrySerde.serGeoJson(
        GeometrySerde.GEOMETRY_FACTORY.createMultiPoint((Point[]) points));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geometry"},
      argTypes = {"Geometry"},
      returnTypes = "Geometry")
  public static String ST_EndPoint(String geometry) {
    return GeometrySerde.serGeoJson(Functions.endPoint(GeometrySerde.deserGeoJson(geometry)));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geometry"},
      argTypes = {"Geometry"},
      returnTypes = "Geometry")
  public static String ST_Envelope(String geometry) {
    return GeometrySerde.serGeoJson(Functions.envelope(GeometrySerde.deserGeoJson(geometry)));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geometry", "uniformDelta"},
      argTypes = {"Geometry", "double"},
      returnTypes = "Geometry")
  public static String ST_Expand(String geometry, double uniformDelta) {
    return GeometrySerde.serGeoJson(
        Functions.expand(GeometrySerde.deserGeoJson(geometry), uniformDelta));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geometry", "deltaX", "deltaY"},
      argTypes = {"Geometry", "double", "double"},
      returnTypes = "Geometry")
  public static String ST_Expand(String geometry, double deltaX, double deltaY) {
    return GeometrySerde.serGeoJson(
        Functions.expand(GeometrySerde.deserGeoJson(geometry), deltaX, deltaY));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geometry", "deltaX", "deltaY", "deltaZ"},
      argTypes = {"Geometry", "double", "double", "double"},
      returnTypes = "Geometry")
  public static String ST_Expand(String geometry, double deltaX, double deltaY, double deltaZ) {
    return GeometrySerde.serGeoJson(
        Functions.expand(GeometrySerde.deserGeoJson(geometry), deltaX, deltaY, deltaZ));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"leftGeometry", "rightGeometry"},
      argTypes = {"Geometry", "Geometry"})
  public static boolean ST_Equals(String leftGeometry, String rightGeometry) {
    return Predicates.equals(
        GeometrySerde.deserGeoJson(leftGeometry), GeometrySerde.deserGeoJson(rightGeometry));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geometry"},
      argTypes = {"Geometry"},
      returnTypes = "Geometry")
  public static String ST_ExteriorRing(String geometry) {
    return GeometrySerde.serGeoJson(Functions.exteriorRing(GeometrySerde.deserGeoJson(geometry)));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geometry"},
      argTypes = {"Geometry"},
      returnTypes = "Geometry")
  public static String ST_FlipCoordinates(String geometry) {
    return GeometrySerde.serGeoJson(
        Functions.flipCoordinates(GeometrySerde.deserGeoJson(geometry)));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geometry"},
      argTypes = {"Geometry"},
      returnTypes = "Geometry")
  public static String ST_Force_2D(String geometry) {
    return GeometrySerde.serGeoJson(Functions.force2D(GeometrySerde.deserGeoJson(geometry)));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geometry"},
      argTypes = {"Geometry"},
      returnTypes = "Geometry")
  public static String ST_Force2D(String geometry) {
    return GeometrySerde.serGeoJson(Functions.force2D(GeometrySerde.deserGeoJson(geometry)));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geometry", "numPoints"},
      argTypes = {"Geometry", "int"},
      returnTypes = "Geometry")
  public static String ST_GeneratePoints(String geometry, int numPoints) {
    return GeometrySerde.serGeoJson(
        Functions.generatePoints(GeometrySerde.deserGeoJson(geometry), numPoints));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geometry", "numPoints", "seed"},
      argTypes = {"Geometry", "int", "long"},
      returnTypes = "Geometry")
  public static String ST_GeneratePoints(String geometry, int numPoints, long seed) {
    return GeometrySerde.serGeoJson(
        Functions.generatePoints(GeometrySerde.deserGeoJson(geometry), numPoints, seed));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geometry", "precision"},
      argTypes = {"Geometry", "int"})
  public static String ST_GeoHash(String geometry, int precision) {
    return Functions.geohash(GeometrySerde.deserGeoJson(geometry), precision);
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geometry", "n"},
      argTypes = {"Geometry", "int"},
      returnTypes = "Geometry")
  public static String ST_GeometryN(String geometry, int n) {
    return GeometrySerde.serGeoJson(Functions.geometryN(GeometrySerde.deserGeoJson(geometry), n));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geometry"},
      argTypes = {"Geometry"})
  public static String ST_GeometryType(String geometry) {
    return Functions.geometryType(GeometrySerde.deserGeoJson(geometry));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geometry"},
      argTypes = {"Geometry"})
  public static boolean ST_HasZ(String geometry) {
    return Functions.hasZ(GeometrySerde.deserGeoJson(geometry));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geom1", "geom2"},
      argTypes = {"Geometry", "Geometry"})
  public static double ST_HausdorffDistance(String geom1, String geom2) {
    return Functions.hausdorffDistance(
        GeometrySerde.deserGeoJson(geom1), GeometrySerde.deserGeoJson(geom2));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geom1", "geom2", "densifyFrac"},
      argTypes = {"Geometry", "Geometry", "double"})
  public static double ST_HausdorffDistance(String geom1, String geom2, double densifyFrac) {
    return Functions.hausdorffDistance(
        GeometrySerde.deserGeoJson(geom1), GeometrySerde.deserGeoJson(geom2), densifyFrac);
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geometry", "n"},
      argTypes = {"Geometry", "int"},
      returnTypes = "Geometry")
  public static String ST_InteriorRingN(String geometry, int n) {
    return GeometrySerde.serGeoJson(
        Functions.interiorRingN(GeometrySerde.deserGeoJson(geometry), n));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"leftGeometry", "rightGeometry"},
      argTypes = {"Geometry", "Geometry"},
      returnTypes = "Geometry")
  public static String ST_Intersection(String leftGeometry, String rightGeometry) {
    return GeometrySerde.serGeoJson(
        Functions.intersection(
            GeometrySerde.deserGeoJson(leftGeometry), GeometrySerde.deserGeoJson(rightGeometry)));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"leftGeometry", "rightGeometry"},
      argTypes = {"Geometry", "Geometry"})
  public static boolean ST_Intersects(String leftGeometry, String rightGeometry) {
    return Predicates.intersects(
        GeometrySerde.deserGeoJson(leftGeometry), GeometrySerde.deserGeoJson(rightGeometry));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geometry"},
      argTypes = {"Geometry"})
  public static boolean ST_IsClosed(String geometry) {
    return Functions.isClosed(GeometrySerde.deserGeoJson(geometry));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geometry"},
      argTypes = {"Geometry"})
  public static boolean ST_IsCollection(String geometry) {
    return Functions.isCollection(GeometrySerde.deserGeoJson(geometry));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geometry"},
      argTypes = {"Geometry"})
  public static boolean ST_IsEmpty(String geometry) {
    return Functions.isEmpty(GeometrySerde.deserGeoJson(geometry));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geom"},
      argTypes = {"Geometry"})
  public static boolean ST_IsPolygonCCW(String geom) {
    return Functions.isPolygonCCW(GeometrySerde.deserGeoJson(geom));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geometry"},
      argTypes = {"Geometry"})
  public static boolean ST_IsPolygonCW(String geom) {
    return Functions.isPolygonCW(GeometrySerde.deserGeoJson(geom));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geometry"},
      argTypes = {"Geometry"})
  public static boolean ST_IsRing(String geometry) {
    return Functions.isRing(GeometrySerde.deserGeoJson(geometry));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geometry"},
      argTypes = {"Geometry"})
  public static boolean ST_IsSimple(String geometry) {
    return Functions.isSimple(GeometrySerde.deserGeoJson(geometry));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geometry"},
      argTypes = {"Geometry"})
  public static boolean ST_IsValid(String geometry) {
    return Functions.isValid(GeometrySerde.deserGeoJson(geometry));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geometry", "flags"},
      argTypes = {"Geometry", "int"})
  public static boolean ST_IsValid(String geometry, int flags) {
    return Functions.isValid(GeometrySerde.deserGeoJson(geometry), flags);
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geometry"},
      argTypes = {"Geometry"})
  public static String ST_IsValidReason(String geometry) {
    return Functions.isValidReason(GeometrySerde.deserGeoJson(geometry));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geometry", "flags"},
      argTypes = {"Geometry", "int"})
  public static String ST_IsValidReason(String geometry, int flags) {
    return Functions.isValidReason(GeometrySerde.deserGeoJson(geometry), flags);
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geometry"},
      argTypes = {"Geometry"})
  public static double ST_Length(String geometry) {
    return Functions.length(GeometrySerde.deserGeoJson(geometry));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geometry"},
      argTypes = {"Geometry"})
  public static double ST_Length2D(String geometry) {
    return Functions.length(GeometrySerde.deserGeoJson(geometry));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geometry"},
      argTypes = {"Geometry"},
      returnTypes = "Geometry")
  public static String ST_LineFromMultiPoint(String geometry) {
    return GeometrySerde.serGeoJson(
        Functions.lineFromMultiPoint(GeometrySerde.deserGeoJson(geometry)));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geom", "fraction"},
      argTypes = {"Geometry", "double"},
      returnTypes = "Geometry")
  public static String ST_LineInterpolatePoint(String geom, double fraction) {
    return GeometrySerde.serGeoJson(
        Functions.lineInterpolatePoint(GeometrySerde.deserGeoJson(geom), fraction));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geom", "point"},
      argTypes = {"Geometry", "Geometry"})
  public static double ST_LineLocatePoint(String geom, String point) {
    return Functions.lineLocatePoint(
        GeometrySerde.deserGeoJson(geom), GeometrySerde.deserGeoJson(point));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geometry"},
      argTypes = {"Geometry"},
      returnTypes = "Geometry")
  public static String ST_LineMerge(String geometry) {
    return GeometrySerde.serGeoJson(Functions.lineMerge(GeometrySerde.deserGeoJson(geometry)));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geom", "fromFraction", "toFraction"},
      argTypes = {"Geometry", "double", "double"},
      returnTypes = "Geometry")
  public static String ST_LineSubstring(String geom, double fromFraction, double toFraction) {
    return GeometrySerde.serGeoJson(
        Functions.lineSubString(GeometrySerde.deserGeoJson(geom), fromFraction, toFraction));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geom1", "geom2"},
      argTypes = {"Geometry", "Geometry"},
      returnTypes = "Geometry")
  public static String ST_LongestLine(String geom1, String geom2) {
    return GeometrySerde.serGeoJson(
        Functions.longestLine(
            GeometrySerde.deserGeoJson(geom1), GeometrySerde.deserGeoJson(geom2)));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"point1", "point2"},
      argTypes = {"Geometry", "Geometry"},
      returnTypes = "Geometry")
  public static String ST_MakeLine(String geom1, String geom2) {
    return GeometrySerde.serGeoJson(
        Functions.makeLine(GeometrySerde.deserGeoJson(geom1), GeometrySerde.deserGeoJson(geom2)));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geometryCollection"},
      argTypes = {"Geometry"},
      returnTypes = "Geometry")
  public static String ST_MakeLine(String geometry) {
    return GeometrySerde.serGeoJson(Functions.makeLine(GeometrySerde.deserGeoJson2List(geometry)));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"shell"},
      argTypes = {"Geometry"},
      returnTypes = "Geometry")
  public static String ST_MakePolygon(String shell) {
    return GeometrySerde.serGeoJson(Functions.makePolygon(GeometrySerde.deserGeoJson(shell), null));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"shell", "holes"},
      argTypes = {"Geometry", "Geometry"},
      returnTypes = "Geometry")
  public static String ST_MakePolygon(String shell, String holes) {
    return GeometrySerde.serGeoJson(
        Functions.makePolygon(
            GeometrySerde.deserGeoJson(shell), GeometrySerde.deserGeoJson2List(holes)));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geometry"},
      argTypes = {"Geometry"},
      returnTypes = "Geometry")
  public static String ST_MakeValid(String geometry) {
    return GeometrySerde.serGeoJson(
        Functions.makeValid(GeometrySerde.deserGeoJson(geometry), false));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geometry", "keepCollapsed"},
      argTypes = {"Geometry", "boolean"},
      returnTypes = "Geometry")
  public static String ST_MakeValid(String geometry, boolean keepCollapsed) {
    return GeometrySerde.serGeoJson(
        Functions.makeValid(GeometrySerde.deserGeoJson(geometry), keepCollapsed));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geom1", "geom2"},
      argTypes = {"Geometry", "Geometry"})
  public static double ST_MaxDistance(String geom1, String geom2) {
    return Functions.maxDistance(
        GeometrySerde.deserGeoJson(geom1), GeometrySerde.deserGeoJson(geom2));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geometry"},
      argTypes = {"Geometry"})
  public static Double ST_MinimumClearance(String geometry) throws IOException {
    return Functions.minimumClearance(GeometrySerde.deserGeoJson(geometry));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geometry"},
      argTypes = {"Geometry"},
      returnTypes = "Geometry")
  public static String ST_MinimumClearanceLine(String geometry) {
    return GeometrySerde.serGeoJson(
        Functions.minimumClearanceLine(GeometrySerde.deserGeoJson(geometry)));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geometry", "quadrantSegments"},
      argTypes = {"Geometry", "int"},
      returnTypes = "Geometry")
  public static String ST_MinimumBoundingCircle(String geometry, int quadrantSegments) {
    return GeometrySerde.serGeoJson(
        Functions.minimumBoundingCircle(GeometrySerde.deserGeoJson(geometry), quadrantSegments));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geometry"},
      argTypes = {"Geometry"},
      returnTypes = "Geometry")
  public static String ST_Multi(String geometry) throws IOException {
    return GeometrySerde.serGeoJson(
        Functions.createMultiGeometryFromOneElement(GeometrySerde.deserGeoJson(geometry)));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geometry"},
      argTypes = {"Geometry"})
  public static int ST_NDims(String geometry) {
    return Functions.nDims(GeometrySerde.deserGeoJson(geometry));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geometry"},
      argTypes = {"Geometry"})
  public static int ST_NPoints(String geometry) {
    return Functions.nPoints(GeometrySerde.deserGeoJson(geometry));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geometry"},
      argTypes = {"Geometry"},
      returnTypes = "Geometry")
  public static String ST_Normalize(String geometry) {
    return GeometrySerde.serGeoJson(Functions.normalize(GeometrySerde.deserGeoJson(geometry)));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geometry"},
      argTypes = {"Geometry"})
  public static int ST_NumGeometries(String geometry) {
    return Functions.numGeometries(GeometrySerde.deserGeoJson(geometry));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geometry"},
      argTypes = {"Geometry"})
  public static Integer ST_NumInteriorRings(String geometry) {
    return Functions.numInteriorRings(GeometrySerde.deserGeoJson(geometry));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geometry"},
      argTypes = {"Geometry"})
  public static Integer ST_NumInteriorRing(String geometry) {
    return Functions.numInteriorRings(GeometrySerde.deserGeoJson(geometry));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"leftGeometry", "rightGeometry"},
      argTypes = {"Geometry", "Geometry"})
  public static boolean ST_OrderingEquals(String leftGeometry, String rightGeometry) {
    return Predicates.orderingEquals(
        GeometrySerde.deserGeoJson(leftGeometry), GeometrySerde.deserGeoJson(rightGeometry));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"leftGeometry", "rightGeometry"},
      argTypes = {"Geometry", "Geometry"})
  public static boolean ST_Overlaps(String leftGeometry, String rightGeometry) {
    return Predicates.overlaps(
        GeometrySerde.deserGeoJson(leftGeometry), GeometrySerde.deserGeoJson(rightGeometry));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geometry", "n"},
      argTypes = {"Geometry", "int"},
      returnTypes = "Geometry")
  public static String ST_PointN(String geometry, int n) {
    return GeometrySerde.serGeoJson(Functions.pointN(GeometrySerde.deserGeoJson(geometry), n));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geometry"},
      argTypes = {"Geometry"},
      returnTypes = "Geometry")
  public static String ST_PointOnSurface(String geometry) {
    return GeometrySerde.serGeoJson(Functions.pointOnSurface(GeometrySerde.deserGeoJson(geometry)));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geometry"},
      argTypes = {"Geometry"},
      returnTypes = "Geometry")
  public static String ST_Points(String geometry) {
    return GeometrySerde.serGeoJson(Functions.points(GeometrySerde.deserGeoJson(geometry)));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geometry", "srid"},
      argTypes = {"Geometry", "int"},
      returnTypes = "Geometry")
  public static String ST_Polygon(String geometry, int srid) {
    return GeometrySerde.serGeoJson(
        Functions.makepolygonWithSRID(GeometrySerde.deserGeoJson(geometry), srid));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geometry"},
      argTypes = {"Geometry"},
      returnTypes = "Geometry")
  public static String ST_Polygonize(String geometry) {
    return GeometrySerde.serGeoJson(Functions.polygonize(GeometrySerde.deserGeoJson(geometry)));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geometry", "precisionScale"},
      argTypes = {"Geometry", "int"},
      returnTypes = "Geometry")
  public static String ST_PrecisionReduce(String geometry, int precisionScale) {
    return GeometrySerde.serGeoJson(
        Functions.reducePrecision(GeometrySerde.deserGeoJson(geometry), precisionScale));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geometry", "precisionScale"},
      argTypes = {"Geometry", "int"},
      returnTypes = "Geometry")
  public static String ST_ReducePrecision(String geometry, int precisionScale) {
    return GeometrySerde.serGeoJson(
        Functions.reducePrecision(GeometrySerde.deserGeoJson(geometry), precisionScale));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"linestring"},
      argTypes = {"Geometry"},
      returnTypes = "Geometry")
  public static String ST_RemovePoint(String linestring) {
    return GeometrySerde.serGeoJson(Functions.removePoint(GeometrySerde.deserGeoJson(linestring)));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"linestring", "position"},
      argTypes = {"Geometry", "int"},
      returnTypes = "Geometry")
  public static String ST_RemovePoint(String linestring, int position) {
    return GeometrySerde.serGeoJson(
        Functions.removePoint(GeometrySerde.deserGeoJson(linestring), position));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geometry"},
      argTypes = {"Geometry"},
      returnTypes = "Geometry")
  public static String ST_Reverse(String geometry) {
    return GeometrySerde.serGeoJson(Functions.reverse(GeometrySerde.deserGeoJson(geometry)));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"input", "level"},
      argTypes = {"Geometry", "int"})
  public static long[] ST_S2CellIDs(String input, int level) {
    return TypeUtils.castLong(Functions.s2CellIDs(GeometrySerde.deserGeoJson(input), level));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geometry"},
      argTypes = {"Geometry"})
  public static int ST_SRID(String geometry) {
    return Functions.getSRID(GeometrySerde.deserGeoJson(geometry));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geometry"},
      argTypes = {"Geometry"})
  public static String ST_AsText(String geometry) {
    return Functions.asWKT(GeometrySerde.deserGeoJson(geometry));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"linestring", "position", "point"},
      argTypes = {"Geometry", "int", "Geometry"},
      returnTypes = "Geometry")
  public static String ST_SetPoint(String linestring, int position, String point) {
    return GeometrySerde.serGeoJson(
        Functions.setPoint(
            GeometrySerde.deserGeoJson(linestring), position, GeometrySerde.deserGeoJson(point)));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geometry", "srid"},
      argTypes = {"Geometry", "int"},
      returnTypes = "Geometry")
  public static String ST_SetSRID(String geometry, int srid) {
    return GeometrySerde.serGeoJson(Functions.setSRID(GeometrySerde.deserGeoJson(geometry), srid));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geometry", "distanceTolerance"},
      argTypes = {"Geometry", "double"},
      returnTypes = "Geometry")
  public static String ST_SimplifyPreserveTopology(String geometry, double distanceTolerance) {
    return GeometrySerde.serGeoJson(
        Functions.simplifyPreserveTopology(
            GeometrySerde.deserGeoJson(geometry), distanceTolerance));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geometry", "distanceTolerance"},
      argTypes = {"Geometry", "double"},
      returnTypes = "Geometry")
  public static String ST_SimplifyVW(String geometry, double distanceTolerance) {
    return GeometrySerde.serGeoJson(
        Functions.simplifyVW(GeometrySerde.deserGeoJson(geometry), distanceTolerance));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geometry", "vertexFactor", "isOuter"},
      argTypes = {"Geometry", "double", "boolean"},
      returnTypes = "Geometry")
  public static String ST_SimplifyPolygonHull(
      String geometry, double vertexFactor, boolean isOuter) {
    return GeometrySerde.serGeoJson(
        Functions.simplifyPolygonHull(GeometrySerde.deserGeoJson(geometry), vertexFactor, isOuter));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geometry", "vertexFactor"},
      argTypes = {"Geometry", "double"},
      returnTypes = "Geometry")
  public static String ST_SimplifyPolygonHull(String geometry, double vertexFactor) {
    return GeometrySerde.serGeoJson(
        Functions.simplifyPolygonHull(GeometrySerde.deserGeoJson(geometry), vertexFactor));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"input", "blade"},
      argTypes = {"Geometry", "Geometry"},
      returnTypes = "Geometry")
  public static String ST_Split(String input, String blade) {
    return GeometrySerde.serGeoJson(
        Functions.split(GeometrySerde.deserGeoJson(input), GeometrySerde.deserGeoJson(blade)));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geometry"},
      argTypes = {"Geometry"},
      returnTypes = "Geometry")
  public static String ST_StartPoint(String geometry) {
    return GeometrySerde.serGeoJson(Functions.startPoint(GeometrySerde.deserGeoJson(geometry)));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"input", "reference", "tolerance"},
      argTypes = {"Geometry", "Geometry", "double"},
      returnTypes = "Geometry")
  public static String ST_Snap(String input, String reference, double tolerance) {
    return GeometrySerde.serGeoJson(
        Functions.snap(
            GeometrySerde.deserGeoJson(input), GeometrySerde.deserGeoJson(reference), tolerance));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geometry", "maxVertices"},
      argTypes = {"Geometry", "int"},
      returnTypes = "Geometry")
  public static String ST_SubDivide(String geometry, int maxVertices) {
    return GeometrySerde.serGeoJson(
        Functions.subDivide(GeometrySerde.deserGeoJson(geometry), maxVertices));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"leftGeom", "rightGeom"},
      argTypes = {"Geometry", "Geometry"},
      returnTypes = "Geometry")
  public static String ST_SymDifference(String leftGeom, String rightGeom) {
    return GeometrySerde.serGeoJson(
        Functions.symDifference(
            GeometrySerde.deserGeoJson(leftGeom), GeometrySerde.deserGeoJson(rightGeom)));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"leftGeometry", "rightGeometry"},
      argTypes = {"Geometry", "Geometry"})
  public static boolean ST_Touches(String leftGeometry, String rightGeometry) {
    return Predicates.touches(
        GeometrySerde.deserGeoJson(leftGeometry), GeometrySerde.deserGeoJson(rightGeometry));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"leftGeometry", "rightGeometry"},
      argTypes = {"Geometry", "Geometry"},
      returnTypes = "String")
  public static String ST_Relate(String leftGeometry, String rightGeometry) {
    return Predicates.relate(
        GeometrySerde.deserGeoJson(leftGeometry), GeometrySerde.deserGeoJson(rightGeometry));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geom1", "geom2", "intersectionMatrix"},
      argTypes = {"Geometry", "Geometry", "String"})
  public static boolean ST_Relate(String geom1, String geom2, String intersectionMatrix) {
    return Predicates.relate(
        GeometrySerde.deserGeoJson(geom1), GeometrySerde.deserGeoJson(geom2), intersectionMatrix);
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"matrix1", "matrix2"},
      argTypes = {"String", "String"})
  public static boolean ST_RelateMatch(String matrix1, String matrix2) {
    return Predicates.relateMatch(matrix1, matrix2);
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geometry", "sourceCRS", "targetCRS"},
      argTypes = {"Geometry", "String", "String"},
      returnTypes = "Geometry")
  public static String ST_Transform(String geometry, String sourceCRS, String targetCRS) {
    return GeometrySerde.serGeoJson(
        GeoToolsWrapper.transform(GeometrySerde.deserGeoJson(geometry), sourceCRS, targetCRS));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geometry", "sourceCRS", "targetCRS", "lenient"},
      argTypes = {"Geometry", "String", "String", "boolean"},
      returnTypes = "Geometry")
  public static String ST_Transform(
      String geometry, String sourceCRS, String targetCRS, boolean lenient) {
    return GeometrySerde.serGeoJson(
        GeoToolsWrapper.transform(
            GeometrySerde.deserGeoJson(geometry), sourceCRS, targetCRS, lenient));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"leftGeom", "rightGeom"},
      argTypes = {"Geometry", "Geometry"},
      returnTypes = "Geometry")
  public static String ST_Union(String leftGeom, String rightGeom) {
    return GeometrySerde.serGeoJson(
        Functions.union(
            GeometrySerde.deserGeoJson(leftGeom), GeometrySerde.deserGeoJson(rightGeom)));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geometry"},
      argTypes = {"Geometry"},
      returnTypes = "Geometry")
  public static String ST_UnaryUnion(String geometry) {
    return GeometrySerde.serGeoJson(Functions.unaryUnion(GeometrySerde.deserGeoJson(geometry)));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geometry"},
      argTypes = {"Geometry"},
      returnTypes = "Geometry")
  public static String ST_VoronoiPolygons(String geometry) {
    return GeometrySerde.serGeoJson(
        FunctionsGeoTools.voronoiPolygons(GeometrySerde.deserGeoJson(geometry), 0.0, null));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geometry", "tolerance"},
      argTypes = {"Geometry", "double"},
      returnTypes = "Geometry")
  public static String ST_VoronoiPolygons(String geometry, double tolerance) {
    return GeometrySerde.serGeoJson(
        FunctionsGeoTools.voronoiPolygons(GeometrySerde.deserGeoJson(geometry), tolerance, null));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geometry", "tolerance", "extent"},
      argTypes = {"Geometry", "double", "Geometry"},
      returnTypes = "Geometry")
  public static String ST_VoronoiPolygons(String geometry, double tolerance, String extent) {
    return GeometrySerde.serGeoJson(
        FunctionsGeoTools.voronoiPolygons(
            GeometrySerde.deserGeoJson(geometry), tolerance, GeometrySerde.deserGeoJson(extent)));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"leftGeometry", "rightGeometry"},
      argTypes = {"Geometry", "Geometry"})
  public static boolean ST_Within(String leftGeometry, String rightGeometry) {
    return Predicates.within(
        GeometrySerde.deserGeoJson(leftGeometry), GeometrySerde.deserGeoJson(rightGeometry));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geometry"},
      argTypes = {"Geometry"})
  public static Double ST_X(String geometry) {
    return Functions.x(GeometrySerde.deserGeoJson(geometry));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geometry"},
      argTypes = {"Geometry"})
  public static double ST_XMax(String geometry) {
    return Functions.xMax(GeometrySerde.deserGeoJson(geometry));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geometry"},
      argTypes = {"Geometry"})
  public static double ST_XMin(String geometry) {
    return Functions.xMin(GeometrySerde.deserGeoJson(geometry));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geometry"},
      argTypes = {"Geometry"})
  public static Double ST_Y(String geometry) {
    return Functions.y(GeometrySerde.deserGeoJson(geometry));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geometry"},
      argTypes = {"Geometry"})
  public static double ST_YMax(String geometry) {
    return Functions.yMax(GeometrySerde.deserGeoJson(geometry));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geometry"},
      argTypes = {"Geometry"})
  public static double ST_YMin(String geometry) {
    return Functions.yMin(GeometrySerde.deserGeoJson(geometry));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geometry"},
      argTypes = {"Geometry"})
  public static Double ST_Z(String geometry) {
    return Functions.z(GeometrySerde.deserGeoJson(geometry));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geometry"},
      argTypes = {"Geometry"})
  public static Double ST_ZMax(String geometry) {
    return Functions.zMax(GeometrySerde.deserGeoJson(geometry));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geometry"},
      argTypes = {"Geometry"})
  public static Double ST_ZMin(String geometry) {
    return Functions.zMin(GeometrySerde.deserGeoJson(geometry));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geometry"},
      argTypes = {"Geometry"})
  public static Double ST_AreaSpheroid(String geometry) {
    return Spheroid.area(GeometrySerde.deserGeoJson(geometry));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geomA", "geomB"},
      argTypes = {"Geometry", "Geometry"})
  public static Double ST_DistanceSphere(String geomA, String geomB) {
    return Haversine.distance(GeometrySerde.deserGeoJson(geomA), GeometrySerde.deserGeoJson(geomB));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geomA", "geomB", "radius"},
      argTypes = {"Geometry", "Geometry", "double"})
  public static Double ST_DistanceSphere(String geomA, String geomB, double radius) {
    return Haversine.distance(
        GeometrySerde.deserGeoJson(geomA), GeometrySerde.deserGeoJson(geomB), radius);
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geomA", "geomB"},
      argTypes = {"Geometry", "Geometry"})
  public static Double ST_DistanceSpheroid(String geomA, String geomB) {
    return Spheroid.distance(GeometrySerde.deserGeoJson(geomA), GeometrySerde.deserGeoJson(geomB));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geomA", "geomB", "distance"},
      argTypes = {"Geometry", "Geometry", "double"})
  public static boolean ST_DWithin(String geomA, String geomB, double distance) {
    return Predicates.dWithin(
        GeometrySerde.deserGeoJson(geomA), GeometrySerde.deserGeoJson(geomB), distance);
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geomA", "geomB"},
      argTypes = {"Geometry", "Geometry"})
  public static double ST_FrechetDistance(String geomA, String geomB) {
    return Functions.frechetDistance(
        GeometrySerde.deserGeoJson(geomA), GeometrySerde.deserGeoJson(geomB));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geom", "zValue"},
      argTypes = {"Geometry", "double"},
      returnTypes = "Geometry")
  public static String ST_Force3D(String geom, double zValue) {
    return GeometrySerde.serGeoJson(Functions.force3D(GeometrySerde.deserGeoJson(geom), zValue));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geom"},
      argTypes = {"Geometry"},
      returnTypes = "Geometry")
  public static String ST_Force3D(String geom) {
    return GeometrySerde.serGeoJson(Functions.force3D(GeometrySerde.deserGeoJson(geom)));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geom", "zValue"},
      argTypes = {"Geometry", "double"},
      returnTypes = "Geometry")
  public static String ST_Force3DZ(String geom, double zValue) {
    return GeometrySerde.serGeoJson(Functions.force3D(GeometrySerde.deserGeoJson(geom), zValue));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geom"},
      argTypes = {"Geometry"},
      returnTypes = "Geometry")
  public static String ST_Force3DZ(String geom) {
    return GeometrySerde.serGeoJson(Functions.force3D(GeometrySerde.deserGeoJson(geom)));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geom"},
      argTypes = {"Geometry"},
      returnTypes = "Geometry")
  public static String ST_ForceCollection(String geom) {
    return GeometrySerde.serGeoJson(Functions.forceCollection(GeometrySerde.deserGeoJson(geom)));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geom"},
      argTypes = {"Geometry"},
      returnTypes = "Geometry")
  public static String ST_ForcePolygonCW(String geom) {
    return GeometrySerde.serGeoJson(Functions.forcePolygonCW(GeometrySerde.deserGeoJson(geom)));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geom"},
      argTypes = {"Geometry"},
      returnTypes = "Geometry")
  public static String ST_ForcePolygonCCW(String geom) {
    return GeometrySerde.serGeoJson(Functions.forcePolygonCCW(GeometrySerde.deserGeoJson(geom)));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geom"},
      argTypes = {"Geometry"},
      returnTypes = "Geometry")
  public static String ST_ForceRHR(String geom) {
    return GeometrySerde.serGeoJson(Functions.forcePolygonCW(GeometrySerde.deserGeoJson(geom)));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geom"},
      argTypes = {"Geometry"})
  public static double ST_LengthSpheroid(String geom) {
    return Spheroid.length(GeometrySerde.deserGeoJson(geom));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geom"},
      argTypes = {"Geometry"},
      returnTypes = "Geometry")
  public static String ST_GeometricMedian(String geom) throws Exception {
    return GeometrySerde.serGeoJson(Functions.geometricMedian(GeometrySerde.deserGeoJson(geom)));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geom", "tolerance"},
      argTypes = {"Geometry", "float"},
      returnTypes = "Geometry")
  public static String ST_GeometricMedian(String geom, float tolerance) throws Exception {
    return GeometrySerde.serGeoJson(
        Functions.geometricMedian(GeometrySerde.deserGeoJson(geom), tolerance));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geom", "tolerance", "maxIter"},
      argTypes = {"Geometry", "float", "int"},
      returnTypes = "Geometry")
  public static String ST_GeometricMedian(String geom, float tolerance, int maxIter)
      throws Exception {
    return GeometrySerde.serGeoJson(
        Functions.geometricMedian(GeometrySerde.deserGeoJson(geom), tolerance, maxIter));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geom"},
      argTypes = {"Geometry"})
  public static int ST_NRings(String geom) throws Exception {
    return Functions.nRings(GeometrySerde.deserGeoJson(geom));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geom"},
      argTypes = {"Geometry"})
  public static int ST_NumPoints(String geom) throws Exception {
    return Functions.numPoints(GeometrySerde.deserGeoJson(geom));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geom"},
      argTypes = {"Geometry"},
      returnTypes = "Geometry")
  public static String ST_TriangulatePolygon(String geom) {
    return GeometrySerde.serGeoJson(Functions.triangulatePolygon(GeometrySerde.deserGeoJson(geom)));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geom", "deltaX", "deltaY"},
      argTypes = {"Geometry", "double", "double"},
      returnTypes = "Geometry")
  public static String ST_Translate(String geom, double deltaX, double deltaY) {
    return GeometrySerde.serGeoJson(
        Functions.translate(GeometrySerde.deserGeoJson(geom), deltaX, deltaY));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geom", "deltaX", "deltaY", "deltaZ"},
      argTypes = {"Geometry", "double", "double", "double"},
      returnTypes = "Geometry")
  public static String ST_Translate(String geom, double deltaX, double deltaY, double deltaZ) {
    return GeometrySerde.serGeoJson(
        Functions.translate(GeometrySerde.deserGeoJson(geom), deltaX, deltaY, deltaZ));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geometry", "angle"},
      argTypes = {"Geometry", "double"},
      returnTypes = "Geometry")
  public static String ST_RotateX(String geometry, double angle) {
    return GeometrySerde.serGeoJson(Functions.rotateX(GeometrySerde.deserGeoJson(geometry), angle));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geom", "angle"},
      argTypes = {"Geometry", "double"},
      returnTypes = "Geometry")
  public static String ST_Rotate(String geom, double angle) {
    return GeometrySerde.serGeoJson(Functions.rotate(GeometrySerde.deserGeoJson(geom), angle));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geom", "angle", "pointOrigin"},
      argTypes = {"Geometry", "double", "Geometry"},
      returnTypes = "Geometry")
  public static String ST_Rotate(String geom, double angle, String pointOrigin) {
    return GeometrySerde.serGeoJson(
        Functions.rotate(
            GeometrySerde.deserGeoJson(geom), angle, GeometrySerde.deserGeoJson(pointOrigin)));
  }

  @UDFAnnotations.ParamMeta(
      argNames = {"geom", "angle", "originX", "originY"},
      argTypes = {"Geometry", "double", "double", "double"},
      returnTypes = "Geometry")
  public static String ST_Rotate(String geom, double angle, double originX, double originY) {
    return GeometrySerde.serGeoJson(
        Functions.rotate(GeometrySerde.deserGeoJson(geom), angle, originX, originY));
  }
}
