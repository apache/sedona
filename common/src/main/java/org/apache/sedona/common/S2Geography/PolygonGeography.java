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

import static org.apache.sedona.common.spider.Generator.GEOMETRY_FACTORY;

import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.UnsafeInput;
import com.esotericsoftware.kryo.io.UnsafeOutput;
import com.google.common.geometry.*;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.impl.CoordinateArraySequence;

public class PolygonGeography extends Geography {
  private static final Logger logger = Logger.getLogger(PolygonGeography.class.getName());

  public final S2Polygon polygon;

  public PolygonGeography() {
    super(GeographyKind.POLYGON);
    this.polygon = new S2Polygon();
  }

  public PolygonGeography(S2Polygon polygon) {
    super(GeographyKind.POLYGON);
    this.polygon = polygon;
  }

  @Override
  public int dimension() {
    return 2;
  }

  @Override
  public int numShapes() {
    return polygon.isEmpty() ? 0 : 1;
  }

  @Override
  public S2Shape shape(int id) {
    assert polygon != null;
    return polygon.shape();
  }

  @Override
  public S2Region region() {
    return this.polygon;
  }

  @Override
  public void getCellUnionBound(List<S2CellId> cellIds) {
    super.getCellUnionBound(cellIds);
  }

  @Override
  public void encode(UnsafeOutput out, EncodeOptions opts) throws IOException {
    polygon.encode(out);
    out.flush();
  }

  /** This is what decodeTagged() actually calls */
  public static PolygonGeography decode(Input in, EncodeTag tag) throws IOException {
    // cast to UnsafeInput—will work if you always pass a Kryo-backed stream
    if (!(in instanceof UnsafeInput)) {
      throw new IllegalArgumentException("Expected UnsafeInput");
    }
    return decode((UnsafeInput) in, tag);
  }

  public static PolygonGeography decode(UnsafeInput in, EncodeTag tag) throws IOException {
    PolygonGeography geo = new PolygonGeography();

    // EMPTY
    if ((tag.getFlags() & EncodeTag.FLAG_EMPTY) != 0) {
      logger.fine("Decoded empty PolygonGeography.");
      return geo;
    }

    // 2) Skip past any covering cell-IDs written by encodeTagged
    tag.skipCovering(in);

    S2Polygon poly = S2Polygon.decode(in);
    geo = new PolygonGeography(poly);
    return geo;
  }

  public CoordinateSequence getCoordinateSequence() {
    // Count total vertices across all loops
    int total = 0;
    for (int li = 0; li < polygon.numLoops(); li++) {
      S2Loop loop = polygon.loop(li);
      total += loop.numVertices();
    }
    // Build array of Coordinates
    Coordinate[] coords = new Coordinate[total];
    int idx = 0;
    for (int li = 0; li < polygon.numLoops(); li++) {
      S2Loop loop = polygon.loop(li);
      int nv = loop.numVertices();
      for (int vi = 0; vi < nv; vi++, idx++) {
        S2Point p = loop.vertex(vi);
        coords[idx] = new Coordinate(p.getX(), p.getY(), p.getZ());
      }
    }
    return new CoordinateArraySequence(coords);
  }

  public LinearRing getExteriorRing() {
    if (polygon.numLoops() == 0) return GEOMETRY_FACTORY.createLinearRing(new Coordinate[0]);
    S2Loop loop = polygon.loop(0);
    Coordinate[] coords = new Coordinate[loop.numVertices() + 1];
    for (int i = 0; i < loop.numVertices(); i++) {
      S2Point pt = loop.vertex(i);
      S2LatLng ll = new S2LatLng(pt);
      double lat = ll.latDegrees();
      double lon = ll.lngDegrees();
      Coordinate c = new Coordinate(lon, lat);
      coords[i] = c;
    }
    coords[coords.length - 1] = coords[0];
    return GEOMETRY_FACTORY.createLinearRing(coords);
  }

  /** Returns all interior loops as JTS LineStrings. */
  public List<LinearRing> getLoops() {
    List<LinearRing> loops = new ArrayList<>();
    for (int li = 1; li < polygon.numLoops(); li++) {
      S2Loop loop = polygon.loop(li);
      Coordinate[] coords = new Coordinate[loop.numVertices() + 1];
      for (int idx = 0; idx < loop.numVertices(); idx++) {
        S2Point pt = loop.vertex(idx);
        S2LatLng ll = new S2LatLng(pt);
        double lat = ll.latDegrees();
        double lon = ll.lngDegrees();
        Coordinate c = new Coordinate(lon, lat);
        coords[idx] = c;
      }
      coords[coords.length - 1] = coords[0];
      loops.add(GEOMETRY_FACTORY.createLinearRing(coords));
    }
    return loops;
  }
}
