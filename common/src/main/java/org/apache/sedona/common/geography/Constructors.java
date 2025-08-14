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
package org.apache.sedona.common.geography;

import com.google.common.geometry.*;
import java.io.IOException;
import java.util.*;
import org.apache.sedona.common.S2Geography.*;
import org.apache.sedona.common.S2Geography.Geography;
import org.apache.sedona.common.S2Geography.WKBReader;
import org.apache.sedona.common.S2Geography.WKTReader;
import org.apache.sedona.common.utils.GeoHashDecoder;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.io.ParseException;

public class Constructors {

  public static Geography geogFromWKB(byte[] wkb) throws ParseException {
    return new WKBReader().read(wkb);
  }

  public static Geography geogFromWKB(byte[] wkb, int SRID) throws ParseException {
    Geography geog = geogFromWKB(wkb);
    geog.setSRID(SRID);
    return geog;
  }

  public static Geography geogFromWKT(String wkt, int srid) throws ParseException {
    Geography geog = new WKTReader().read(wkt);
    geog.setSRID(srid);
    return geog;
  }

  public static Geography geogFromEWKT(String ewkt) throws ParseException {
    if (ewkt == null) {
      return null;
    }
    int SRID = 0;
    String wkt = ewkt;

    int index = ewkt.indexOf("SRID=");
    if (index != -1) {
      int semicolonIndex = ewkt.indexOf(';', index);
      if (semicolonIndex != -1) {
        SRID = Integer.parseInt(ewkt.substring(index + 5, semicolonIndex));
        wkt = ewkt.substring(semicolonIndex + 1);
      } else {
        throw new ParseException("Invalid EWKT string");
      }
    }
    return geogFromWKT(wkt, SRID);
  }

  public static Geography geogCollFromText(String wkt, int srid) throws ParseException {
    if (wkt == null || !wkt.startsWith("GEOMETRYCOLLECTION")) {
      return null;
    }
    return geogFromWKT(wkt, srid);
  }

  public static Geography geogFromGeoHash(String geoHash, Integer precision) {
    try {
      return GeoHashDecoder.decodeGeog(geoHash, precision);
    } catch (GeoHashDecoder.InvalidGeoHashException e) {
      return null;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static Geometry geogToGeometry(Geography geography, int srid) {
    int SRID = srid == geography.getSRID() ? srid : geography.getSRID();
    GeometryFactory geometryFactory = new GeometryFactory(new PrecisionModel(), SRID);
    return geogToGeometry(geography, geometryFactory);
  }

  public static Geometry geogToGeometry(Geography geography, GeometryFactory geometryFactory) {
    if (geography == null) return null;
    Geography.GeographyKind kind = Geography.GeographyKind.fromKind(geography.getKind());
    switch (kind) {
      case SINGLEPOINT:
      case POINT:
        return pointToGeom(geography, geometryFactory);
      case SINGLEPOLYLINE:
      case POLYLINE:
        return polylineToGeom(geography, geometryFactory);
      case POLYGON:
      case MULTIPOLYGON:
        return polygonToGeom(geography, geometryFactory);
      case GEOGRAPHY_COLLECTION:
        return collectionToGeom(geography, geometryFactory);
      default:
        return null;
    }
  }

  // POINT/SINGLEPOINT
  private static Geometry pointToGeom(Geography g, GeometryFactory gf) {
    if (g instanceof SinglePointGeography) {
      S2Point p = ((SinglePointGeography) g).getPoints().get(0);
      S2LatLng ll = S2LatLng.fromPoint(p);
      return gf.createPoint(new Coordinate(ll.lngDegrees(), ll.latDegrees()));
    } else if (g instanceof PointGeography) {
      List<S2Point> pts = ((PointGeography) g).getPoints();
      Coordinate[] cs = new Coordinate[pts.size()];
      for (int i = 0; i < pts.size(); i++) {
        S2LatLng ll = S2LatLng.fromPoint(pts.get(i));
        cs[i] = new Coordinate(ll.lngDegrees(), ll.latDegrees());
      }
      return gf.createMultiPointFromCoords(cs);
    }
    return null;
  }

  // POLYLINE/SINGLEPOLYLINE
  private static Geometry polylineToGeom(Geography g, GeometryFactory gf) {
    if (g instanceof SinglePolylineGeography) {
      S2Polyline line = ((SinglePolylineGeography) g).getPolylines().get(0);
      int n = line.numVertices();
      Coordinate[] cs = new Coordinate[n];
      for (int k = 0; k < n; k++) {
        S2LatLng ll = S2LatLng.fromPoint(line.vertex(k));
        cs[k] = new Coordinate(ll.lngDegrees(), ll.latDegrees());
      }
      return gf.createLineString(cs);
    } else if (g instanceof PolylineGeography) {
      List<S2Polyline> lines = ((PolylineGeography) g).getPolylines();
      LineString[] lss = new LineString[lines.size()];
      for (int i = 0; i < lines.size(); i++) {
        S2Polyline pl = lines.get(i);
        int n = pl.numVertices();
        Coordinate[] cs = new Coordinate[n];
        for (int k = 0; k < n; k++) {
          S2LatLng ll = S2LatLng.fromPoint(pl.vertex(k));
          cs[k] = new Coordinate(ll.lngDegrees(), ll.latDegrees());
        }
        lss[i] = gf.createLineString(cs);
      }
      return gf.createMultiLineString(lss);
    }
    return null;
  }

  // POLYGON / MULTIPOLYGON
  private static Geometry polygonToGeom(Geography g, GeometryFactory gf) {
    if (g instanceof PolygonGeography) {
      S2Polygon s2p = ((PolygonGeography) g).polygon;
      return s2LoopsToJts(s2p.getLoops(), gf);
    } else if (g instanceof MultiPolygonGeography) {
      List<Geography> parts = ((MultiPolygonGeography) g).getFeatures();
      Polygon[] polys = new Polygon[parts.size()];
      for (int i = 0; i < parts.size(); i++) {
        polys[i] = (Polygon) s2LoopsToJts(((PolygonGeography) parts.get(i)).polygon.getLoops(), gf);
      }
      return gf.createMultiPolygon(polys);
    }
    return null;
  }

  private static Geometry s2LoopsToJts(List<S2Loop> loops, GeometryFactory gf) {
    if (loops == null || loops.isEmpty()) return gf.createPolygon();

    List<LinearRing> shells = new ArrayList<>();
    List<List<LinearRing>> holesPerShell = new ArrayList<>();

    // Stack of current ancestor shells: each frame = {shellIndex, depth}
    //    depth 0: Shell A
    //          depth 1: Hole H1  (a lake in A)
    //          depth 2: Shell S2 (an island in that lake A)
    //                depth 3: Hole H3 (a pond on that island)
    //    depth 0: Shell B     (disjoint area)
    //          depth 1: Hole H2   (a lake in B)
    List<int[]> shellStack = new ArrayList<>();

    for (S2Loop L : loops) {
      int n = L.numVertices();
      if (n < 3) continue;

      // Build & close ring once (x=lng, y=lat)
      Coordinate[] cs = new Coordinate[n + 1];
      for (int i = 0; i < n; i++) {
        S2LatLng ll = S2LatLng.fromPoint(L.vertex(i)).normalized();
        cs[i] = new Coordinate(ll.lngDegrees(), ll.latDegrees());
      }
      cs[n] = cs[0];

      // Guard against degenerate collapse
      if (cs.length < 4 || cs[0].equals2D(cs[1]) || cs[1].equals2D(cs[2])) continue;

      LinearRing ring = gf.createLinearRing(cs);

      boolean isShell = (L.depth() & 1) == 0;
      int depth = L.depth();

      // Unwind ancestors until parent depth < current depth
      while (!shellStack.isEmpty() && shellStack.get(shellStack.size() - 1)[1] >= depth) {
        shellStack.remove(shellStack.size() - 1);
      }

      if (isShell) {
        // New shell => new polygon component
        shells.add(ring);
        holesPerShell.add(new ArrayList<>());
        shellStack.add(new int[] {shells.size() - 1, depth});
      } else {
        ring = ensureOrientation(ring, /*wantCCW=*/ false, gf);
        // Attach hole to nearest even-depth ancestor shell
        if (!shellStack.isEmpty()) {
          int[] shellContainer = shellStack.get(shellStack.size() - 1);
          holesPerShell.get(shellContainer[0]).add(ring);
        }
        // If no ancestor shell (invalid structure), ignore the hole.
      }
    }

    if (shells.isEmpty()) return gf.createPolygon();
    if (shells.size() == 1) {
      Polygon polygon =
          gf.createPolygon(shells.get(0), holesPerShell.get(0).toArray(new LinearRing[0]));
      return polygon;
    }
    Polygon[] polys = new Polygon[shells.size()];
    for (int i = 0; i < shells.size(); i++) {
      polys[i] = gf.createPolygon(shells.get(i), holesPerShell.get(i).toArray(new LinearRing[0]));
    }
    return gf.createMultiPolygon(polys);
  }

  private static LinearRing ensureOrientation(
      LinearRing ring, boolean wantCCW, GeometryFactory gf) {
    boolean isCCW = org.locationtech.jts.algorithm.Orientation.isCCW(ring.getCoordinates());
    // If the actual orientation doesn't match the desired orientation, fix it.
    if (isCCW != wantCCW) {
      Coordinate[] cs = CoordinateArrays.copyDeep(ring.getCoordinates());
      CoordinateArrays.reverse(cs);
      return gf.createLinearRing(cs);
    }
    // Otherwise, the ring is already correctly oriented, so return it as is.
    return ring;
  }

  // COLLECTION
  private static Geometry collectionToGeom(Geography g, GeometryFactory gf) {
    List<Geography> parts = ((GeographyCollection) g).getFeatures();
    Geometry[] gs = new Geometry[parts.size()];
    for (int i = 0; i < parts.size(); i++) {
      gs[i] = geogToGeometry(parts.get(i), gf.getSRID());
    }
    return gf.createGeometryCollection(gs);
  }
}
