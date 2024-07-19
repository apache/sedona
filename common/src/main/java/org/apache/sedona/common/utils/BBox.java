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

import org.locationtech.jts.geom.*;

public class BBox {
  double startLon;
  double endLon;
  double startLat;
  double endLat;
  double minZ;
  double maxZ;

  static GeometryFactory geometryFactory = new GeometryFactory();

  public BBox(double startLon, double endLon, double startLat, double endLat) {
    this.startLon = startLon;
    this.endLon = endLon;
    this.startLat = startLat;
    this.endLat = endLat;
  }

  public BBox(Geometry geom) {
    startLon = Double.MAX_VALUE;
    endLon = -Double.MAX_VALUE;
    startLat = Double.MAX_VALUE;
    endLat = -Double.MAX_VALUE;
    minZ = Double.MAX_VALUE;
    maxZ = -Double.MAX_VALUE;
    processEnvelope(geom);
  }

  private void processEnvelope(Geometry geom) {
    if (geom.getClass().getSimpleName().equals(Geometry.TYPENAME_POINT)) {
      processEnvelopePoint((Point) geom);
    } else if (geom.getClass().getSimpleName().equals(Geometry.TYPENAME_LINESTRING)) {
      processEnvelopeCoordinate(geom.getCoordinates());
    } else if (geom.getClass().getSimpleName().equals(Geometry.TYPENAME_POLYGON)) {
      processEnvelopeCoordinate(((Polygon) geom).getExteriorRing().getCoordinates());
    } else if (geom.getClass().getSimpleName().equals(Geometry.TYPENAME_MULTIPOINT)) {
      for (int i = 0; i < geom.getNumGeometries(); i++) {
        processEnvelope(geom.getGeometryN(i));
      }
    } else if (geom.getClass().getSimpleName().equals(Geometry.TYPENAME_MULTILINESTRING)) {
      for (int i = 0; i < geom.getNumGeometries(); i++) {
        processEnvelope(geom.getGeometryN(i));
      }
    } else if (geom.getClass().getSimpleName().equals(Geometry.TYPENAME_MULTIPOLYGON)) {
      for (int i = 0; i < geom.getNumGeometries(); i++) {
        processEnvelope(geom.getGeometryN(i));
      }
    } else if (geom.getClass().getSimpleName().equals(Geometry.TYPENAME_GEOMETRYCOLLECTION)) {
      for (int i = 0; i < geom.getNumGeometries(); i++) {
        processEnvelope(geom.getGeometryN(i));
      }
    } else {
      throw new RuntimeException("Unknown geometry type: " + geom.getGeometryType());
    }
  }

  private void processEnvelopeCoordinate(Coordinate[] coords) {
    for (Coordinate coord : coords) {
      startLon = Math.min(startLon, coord.x);
      endLon = Math.max(endLon, coord.x);
      startLat = Math.min(startLat, coord.y);
      endLat = Math.max(endLat, coord.y);
      minZ = Math.min(minZ, coord.z);
      maxZ = Math.max(maxZ, coord.z);
    }
  }

  private void processEnvelopePoint(Point geom) {
    startLon = Math.min(startLon, geom.getCoordinates()[0].getX());
    endLon = Math.max(endLon, geom.getCoordinates()[0].getX());
    startLat = Math.min(startLat, geom.getCoordinates()[0].getY());
    endLat = Math.max(endLat, geom.getCoordinates()[0].getY());
    minZ = Math.min(minZ, geom.getCoordinates()[0].getZ());
    maxZ = Math.max(maxZ, geom.getCoordinates()[0].getZ());
  }

  public BBox(BBox other) {
    this(other.startLon, other.endLon, other.startLat, other.endLat);
  }

  public Point getCentroid() {
    double lon = this.startLon + ((this.startLon + this.endLon) / 2);
    double lat = this.startLat + ((this.startLat + this.endLat) / 2);
    return geometryFactory.createPoint(new Coordinate(lon, lat));
  }

  public Polygon toPolygon() {
    return geometryFactory.createPolygon(
        new Coordinate[] {
          new Coordinate(this.startLon, this.startLat),
          new Coordinate(this.startLon, this.endLat),
          new Coordinate(this.endLon, this.endLat),
          new Coordinate(this.endLon, this.startLat),
          new Coordinate(this.startLon, this.startLat)
        });
  }

  public double getStartLon() {
    return startLon;
  }

  public double getEndLon() {
    return endLon;
  }

  public double getStartLat() {
    return startLat;
  }

  public double getEndLat() {
    return endLat;
  }

  public double getMinZ() {
    return minZ;
  }

  public double getMaxZ() {
    return maxZ;
  }
}
