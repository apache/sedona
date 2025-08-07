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

import com.google.common.geometry.*;

public class Accessors {

  public Accessors() {}

  public static boolean S2_isEmpty(Geography geography) {
    for (int i = 0; i < geography.numShapes(); i++) {
      S2Shape shape = geography.shape(i);
      if (!shape.isEmpty()) return false;
    }
    return true;
  }

  public static boolean S2_isCollection(PolygonGeography polygonGeography) {
    int numOuterLoops = 0;
    for (int i = 0; i < polygonGeography.polygon.numLoops(); i++) {
      S2Loop loop = polygonGeography.polygon.loop(i);
      if (loop.depth() == 0 && ++numOuterLoops > 1) {
        return true;
      }
    }
    return false;
  }

  /**
   * Returns true if `geog` is a “collection” (i.e. multi‐point, multi‐linestring, or multi‐polygon)
   * rather than a single simple feature.
   */
  public boolean S2_isCollection(Geography geog) {
    int dim = S2_dimension(geog);
    if (dim == -1) {
      return false;
    }

    switch (dim) {
      case 0:
        // point‐collection if more than one point
        return S2_numPoints(geog) > 1;
      case 1:
        // multi‐linestring: more than one chain across all shapes
        int chainCount = 0;
        for (int i = 0, n = geog.numShapes(); i < n; ++i) {
          S2Shape shape = geog.shape(i);
          chainCount += shape.numChains();
          if (chainCount > 1) {
            return true;
          }
        }
        return false;
      default:
        // polygons (or mixed): delegate to the polygon routine
        if (geog instanceof PolygonGeography) {
          return S2_isCollection((PolygonGeography) geog);
        } else
          throw new IllegalArgumentException(
              "Cannot determine collection status for geography type: "
                  + geog.getClass().getName());
    }
  }

  public static int S2_dimension(Geography geography) {
    int dimension = geography.dimension();
    if (dimension != -1) return dimension;

    for (int i = 0; i < geography.numShapes(); i++) {
      S2Shape shape = geography.shape(i);
      if (shape.dimension() > dimension) dimension = shape.dimension();
    }
    return dimension;
  }

  public static int S2_numPoints(Geography geography) {
    int numPoints = 0;
    for (int i = 0; i < geography.numShapes(); i++) {
      S2Shape shape = geography.shape(i);
      switch (shape.dimension()) {
        case 0:
        case 2:
          numPoints += shape.numEdges();
          break;
        case 1:
          numPoints += shape.numEdges() + shape.numChains();
          break;
      }
    }
    return numPoints;
  }

  double S2_area(Geography geog) {
    if (S2_dimension(geog) != 2) return 0;
    switch (geog.kind) {
      case POLYGON:
        if (geog != null) return S2_area((PolygonGeography) geog);
      case GEOGRAPHY_COLLECTION:
        if (geog != null) return S2_area((GeographyCollection) geog);
      default:
        throw new IllegalArgumentException("Unsupported geography kind for area: " + geog.kind);
    }
  }

  public double S2_area(GeographyCollection geographyCollection) {
    double area = 0;
    for (Geography geography : geographyCollection.features) {
      area += S2_area(geography);
    }
    return area;
  }

  double S2_area(PolygonGeography polygonGeography) {
    return polygonGeography.polygon.getArea();
  }

  public double s2_length(Geography geog) {
    double length = 0.0;
    if (S2_dimension(geog) == 1) {
      for (int i = 0, n = geog.numShapes(); i < n; ++i) {
        S2Shape shape = geog.shape(i);
        for (int j = 0, m = shape.numEdges(); j < m; ++j) {
          S2Shape.MutableEdge edge = new S2Shape.MutableEdge();
          shape.getEdge(j, edge);
          // chord‐angle between the two endpoints
          S1ChordAngle angle = new S1ChordAngle(edge.a, edge.b);
          length += angle.radians();
        }
      }
    }
    return length;
  }

  public double s2_perimeter(Geography geog) {
    double perimeter = 0.0;
    if (S2_dimension(geog) == 2) {
      for (int i = 0, n = geog.numShapes(); i < n; ++i) {
        S2Shape shape = geog.shape(i);
        for (int j = 0, m = shape.numEdges(); j < m; ++j) {
          S2Shape.MutableEdge edge = new S2Shape.MutableEdge();
          shape.getEdge(j, edge);
          // chord‐angle between the two endpoints
          S1ChordAngle angle = new S1ChordAngle(edge.a, edge.b);
          perimeter += angle.radians();
        }
      }
    }
    return perimeter;
  }

  public static double s2_X(Geography geog) {
    double out = Double.NaN;
    for (int i = 0, n = geog.numShapes(); i < n; ++i) {
      S2Shape shape = geog.shape(i);
      if (shape.dimension() == 0 && shape.numEdges() == 1) {
        S2Shape.MutableEdge edge = new S2Shape.MutableEdge();
        shape.getEdge(0, edge);
        if (Double.isNaN(out)) {
          out = S2LatLng.fromPoint(edge.a).lng().degrees();
        } else {
          // second point found → ambiguous
          return Double.NaN;
        }
      }
    }
    return out;
  }

  /**
   * Extract the Y coordinate (latitude in degrees) if this is exactly one point; otherwise returns
   * NaN.
   */
  public static double s2_Y(Geography geog) {
    double out = Double.NaN;
    for (int i = 0, n = geog.numShapes(); i < n; ++i) {
      S2Shape shape = geog.shape(i);
      if (shape.dimension() == 0 && shape.numEdges() == 1) {
        S2Shape.MutableEdge edge = new S2Shape.MutableEdge();
        shape.getEdge(0, edge);
        if (Double.isNaN(out)) {
          out = S2LatLng.fromPoint(edge.a).lat().degrees();
        } else {
          return Double.NaN;
        }
      }
    }
    return out;
  }

  /**
   * Runs S2 validation on a PolylineGeography, writing into the provided S2Error and returning true
   * if an error was found.
   */
  public static boolean s2FindValidationError(PolylineGeography geog, S2Error error) {
    for (S2Polyline polyline : geog.getPolylines()) {
      if (polyline.findValidationError(error)) {
        return true;
      }
    }
    return false;
  }

  /** Runs S2 validation on a PolygonGeography. */
  public static boolean s2FindValidationError(PolygonGeography geog, S2Error error) {
    return geog.polygon.findValidationError(error);
  }

  /** Runs S2 validation on each member of a GeographyCollection. */
  public static boolean s2FindValidationError(GeographyCollection geog, S2Error error) {
    for (Geography feature : geog.getFeatures()) {
      if (s2FindValidationError(feature, error)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Master dispatch for any Geography type: points, lines, polygons, or collections. Follows your
   * C++ logic: 0-dim → always OK 1-dim → polyline path 2-dim → polygon path else → treat as
   * collection of polygons
   */
  public static boolean s2FindValidationError(Geography geog, S2Error error) {
    int dim = geog.dimension();
    switch (dim) {
      case 0:
        // reset error to “OK”
        error.clear();
        return false;
      case 1:
        if (geog instanceof PolylineGeography) {
          return s2FindValidationError((PolylineGeography) geog, error);
        }
        throw new IllegalArgumentException(
            "Expected PolylineGeography for dimension 1, but got: " + geog.getClass().getName());
      case 2:
        if (geog instanceof PolygonGeography) {
          return s2FindValidationError((PolygonGeography) geog, error);
        }
        throw new IllegalArgumentException(
            "Expected PolygonGeography for dimension 2, but got: " + geog.getClass().getName());
      default:
        if (geog instanceof GeographyCollection) {
          return s2FindValidationError((GeographyCollection) geog, error);
        }
        throw new IllegalArgumentException(
            "Unsupported geography type for validation: " + geog.getClass().getName());
    }
  }
}
