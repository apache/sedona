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
package org.apache.sedona.common.spider;

import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import org.apache.sedona.common.enums.GeometryType;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;

abstract class PointBasedGenerator implements Generator {

  public static class PointBasedParameter {
    public final GeometryType geometryType;
    public final RandomBoxParameter boxParameter;
    public final RandomPolygonParameter polygonParameter;

    public PointBasedParameter(
        GeometryType geometryType,
        RandomBoxParameter boxParameter,
        RandomPolygonParameter polygonParameter) {
      this.geometryType = geometryType;
      this.boxParameter = boxParameter;
      this.polygonParameter = polygonParameter;
    }

    public static PointBasedParameter create(Map<String, String> conf) {
      String geomType = conf.getOrDefault("geometryType", "point").toLowerCase(Locale.ROOT);
      GeometryType geometryType;
      switch (geomType) {
        case "point":
          geometryType = GeometryType.POINT;
          break;
        case "polygon":
          geometryType = GeometryType.POLYGON;
          break;
        case "box":
          geometryType = GeometryType.RECTANGLE;
          break;
        default:
          throw new IllegalArgumentException("Unsupported geometry type: " + geomType);
      }
      RandomBoxParameter boxParameter = RandomBoxParameter.create(conf);
      RandomPolygonParameter polygonParameter = RandomPolygonParameter.create(conf);
      return new PointBasedParameter(geometryType, boxParameter, polygonParameter);
    }
  }

  public static class RandomBoxParameter {
    public final double maxWidth;
    public final double maxHeight;

    public RandomBoxParameter(double maxWidth, double maxHeight) {
      if (maxWidth < 0) {
        throw new IllegalArgumentException("maxWidth must be non-negative");
      }
      if (maxHeight < 0) {
        throw new IllegalArgumentException("maxHeight must be non-negative");
      }
      this.maxWidth = maxWidth;
      this.maxHeight = maxHeight;
    }

    public static RandomBoxParameter create(Map<String, String> conf) {
      double maxWidth = Double.parseDouble(conf.getOrDefault("maxWidth", "0.01"));
      double maxHeight = Double.parseDouble(conf.getOrDefault("maxHeight", "0.01"));
      return new RandomBoxParameter(maxWidth, maxHeight);
    }
  }

  public static class RandomPolygonParameter {
    public final double maxSize;
    public final int minSegments;
    public final int maxSegments;

    public RandomPolygonParameter(double maxSize, int minSegments, int maxSegments) {
      if (maxSize < 0) {
        throw new IllegalArgumentException("maxSize must be non-negative");
      }
      if (minSegments < 3) {
        throw new IllegalArgumentException("minSegments must be at least 3");
      }
      if (maxSegments < minSegments) {
        throw new IllegalArgumentException("maxSegments must be at least minSegments");
      }
      this.maxSize = maxSize;
      this.minSegments = minSegments;
      this.maxSegments = maxSegments;
    }

    public static RandomPolygonParameter create(Map<String, String> conf) {
      double maxSize = Double.parseDouble(conf.getOrDefault("maxSize", "0.01"));
      int minSegments = Integer.parseInt(conf.getOrDefault("minSegments", "3"));
      int maxSegments = Integer.parseInt(conf.getOrDefault("maxSegments", "3"));
      return new RandomPolygonParameter(maxSize, minSegments, maxSegments);
    }
  }

  private final Random random;
  private final GeometryType geometryType;
  private final RandomBoxParameter boxParameter;
  private final RandomPolygonParameter polygonParameter;

  public PointBasedGenerator(Random random, PointBasedParameter pointBasedParameter) {
    this.random = random;
    this.geometryType = pointBasedParameter.geometryType;
    this.boxParameter = pointBasedParameter.boxParameter;
    this.polygonParameter = pointBasedParameter.polygonParameter;
  }

  /** Generate a random value {0, 1} from a bernoulli distribution with parameter p */
  protected int bernoulli(double p) {
    return random.nextDouble() < p ? 1 : 0;
  }

  /** Generate a random value in the range [a, b) from a uniform distribution */
  protected double uniform(double a, double b) {
    return (b - a) * random.nextDouble() + a;
  }

  /** Generate a random number in the range (-inf, +inf) from a normal distribution */
  protected double normal(double mu, double sigma) {
    return mu
        + sigma
            * Math.sqrt(-2 * Math.log(random.nextDouble()))
            * Math.sin(2 * Math.PI * random.nextDouble());
  }

  /** Generate a random integer in the range [1, n] */
  protected int dice(int n) {
    return dice(1, n);
  }

  /** Generate a random integer in the given range */
  protected int dice(int min, int max) {
    return random.nextInt(max - min + 1) + min;
  }

  protected abstract Coordinate generateCoordinate();

  @Override
  public boolean hasNext() {
    return true;
  }

  @Override
  public Geometry next() {
    switch (geometryType) {
      case POINT:
        return generatePoint();
      case POLYGON:
        return generatePolygon();
      case RECTANGLE:
        return generateBox();
      default:
        throw new UnsupportedOperationException("Unsupported geometry type: " + geometryType);
    }
  }

  protected Point generatePoint() {
    Coordinate coordinate = generateCoordinate();
    return GEOMETRY_FACTORY.createPoint(coordinate);
  }

  protected Polygon generateBox() {
    Coordinate coordinate = generateCoordinate();
    double width = uniform(0, boxParameter.maxWidth);
    double height = uniform(0, boxParameter.maxHeight);
    double lowerLeftX = coordinate.x - width / 2;
    double lowerLeftY = coordinate.y - height / 2;
    return GEOMETRY_FACTORY.createPolygon(
        new Coordinate[] {
          new Coordinate(lowerLeftX, lowerLeftY),
          new Coordinate(lowerLeftX + width, lowerLeftY),
          new Coordinate(lowerLeftX + width, lowerLeftY + height),
          new Coordinate(lowerLeftX, lowerLeftY + height),
          new Coordinate(lowerLeftX, lowerLeftY)
        });
  }

  protected Polygon generatePolygon() {
    Coordinate coordinate = generateCoordinate();

    // Generate a polygon around the point as follows
    // Picture a clock with one hand making a complete rotation of 2 PI
    // The hand makes n stops to create n points on the polygon
    // At each stop, we choose a point on the hand at random to create one point
    // We connect all points to create the n line segments
    // This way, we can generate an arbitrary polygon (convex or concave) that does not
    // intersect itself.
    int numSegments = dice(polygonParameter.minSegments, polygonParameter.maxSegments);

    // The sorted angle stops of the hand
    double[] angles = new double[numSegments];
    for (int k = 0; k < numSegments; k++) {
      angles[k] = uniform(0, Math.PI * 2);
    }
    Arrays.sort(angles);

    Coordinate[] coordinates = new Coordinate[numSegments + 1];
    for (int k = 0; k < numSegments; k++) {
      double angle = angles[k];
      double distance = uniform(0, polygonParameter.maxSize / 2);
      double x = coordinate.x + distance * Math.cos(angle);
      double y = coordinate.y + distance * Math.sin(angle);
      coordinates[k] = new Coordinate(x, y);
    }

    // To ensure that the polygon is closed, we override the value of the last point to be
    // the same as the first
    coordinates[numSegments] = new Coordinate(coordinates[0]);
    return GEOMETRY_FACTORY.createPolygon(coordinates);
  }
}
