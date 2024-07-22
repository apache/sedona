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

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;

public class BBox {
  double startLon;
  double endLon;
  double startLat;
  double endLat;

  static GeometryFactory geometryFactory = new GeometryFactory();

  public BBox(double startLon, double endLon, double startLat, double endLat) {
    this.startLon = startLon;
    this.endLon = endLon;
    this.startLat = startLat;
    this.endLat = endLat;
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
}
