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
import com.google.common.geometry.Projection.*;

public class Projection {

  /** Equirectangular (Plate Carree) projection with world width 360 degrees. */
  public static PlateCarreeProjection lngLat() {
    // width = 180 degrees => full world spans [-180,180]
    return new PlateCarreeProjection(180);
  }

  /** Web Mercator (Pseudo Mercator) projection using WGS84 radius. */
  public static MercatorProjection pseudoMercator() {
    // half-circumference of WGS84 ellipsoid: PI * 6378137
    double semiCircumference = Math.PI * 6378137.0;
    return new MercatorProjection(semiCircumference);
  }

  public static class OrthographicProjection extends Projection {
    private S2LatLng centre;
    private S2Point zAxis = new S2Point(0, 0, 1);
    private S2Point yAxis = new S2Point(0, 1, 0);

    OrthographicProjection(S2LatLng centre) {
      zAxis = new S2Point(0, 0, 1);
      yAxis = new S2Point(0, 1, 0);
      this.centre = centre;
    }

    public R2Vector project(S2Point p) {

      S2Point out = S2Point.rotate(p, zAxis, -centre.lngRadians());
      out = S2Point.rotate(out, yAxis, centre.latRadians());
      if (out.getX() >= 0) {
        return new R2Vector(out.getY(), out.getZ());
      } else {
        return new R2Vector();
      }
    }

    public S2Point unProject(R2Vector p) {
      if (Double.isNaN(p.x()) || Double.isNaN(p.y())) {
        throw new IllegalArgumentException(
            "Cannot unproject non-finite point in OrthographicProjection");
      }

      double y = p.x();
      double z = p.y();
      double xy2 = y * y + z * z;
      if (xy2 > 1.0) {
        throw new IllegalArgumentException("Point outside unit circle for orthographic");
      }
      double x = Math.sqrt(1.0 - xy2);
      S2Point out = new S2Point(x, y, z);
      out = S2Point.rotate(out, yAxis, -centre.latRadians());
      out = S2Point.rotate(out, zAxis, centre.lngRadians());
      return out;
    }

    public R2Vector fromLatLng(S2LatLng ll) {
      return project(ll.toPoint());
    }

    public S2LatLng toLatLng(R2Vector p) {
      return S2LatLng.fromPoint(unProject(p));
    }

    public S2Point wrapDistance() {
      return new S2Point(0, 0, 0);
    }
  }
}
