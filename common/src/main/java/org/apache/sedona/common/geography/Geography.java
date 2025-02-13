package org.apache.sedona.common.geography;

import org.locationtech.jts.geom.Geometry;

class Geography {
  private Geometry geometry;

  Geography(Geometry geometry) {
    this.geometry = geometry;
  }

  public Geometry getGeometry() {
    return this.geometry;
  }
}
