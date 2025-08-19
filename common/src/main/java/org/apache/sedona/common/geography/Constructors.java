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

  public static Geometry geogToGeometry(Geography geography) {
    GeometryFactory geometryFactory =
        new GeometryFactory(new PrecisionModel(), geography.getSRID());
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
        throw new IllegalArgumentException("Unsupported Geography type: " + kind);
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
      gs[i] = geogToGeometry(parts.get(i));
    }
    return gf.createGeometryCollection(gs);
  }

  public static Geography geomToGeography(Geometry geom) throws IOException {
    if (geom == null) {
      return null;
    }
    String type = geom.getGeometryType();
    if ("Point".equalsIgnoreCase(type)) {
      return pointToGeog(geom);
    } else if ("MultiPoint".equalsIgnoreCase(type)) {
      return mPointToGeog(geom);
    } else if ("LineString".equalsIgnoreCase(type)) {
      return lineToGeog(geom);
    } else if ("MultiLineString".equalsIgnoreCase(type)) {
      return mLineToGeog(geom);
    } else if ("Polygon".equalsIgnoreCase(type)) {
      return polyToGeog(geom);
    } else if ("MultiPolygon".equalsIgnoreCase(type)) {
      return mPolyToGeog(geom);
    } else if ("GeometryCollection".equalsIgnoreCase(type)) {
      return geomCollToGeog(geom);
    } else {
      throw new UnsupportedOperationException("Unsupported geometry type: " + type);
    }
  }

  private static Geography pointToGeog(Geometry geom) throws IOException {
    Coordinate[] pts = geom.getCoordinates();
    if (pts.length == 0 || Double.isNaN(pts[0].x) || Double.isNaN(pts[0].y)) {
      return new SinglePointGeography();
    }
    double lon = pts[0].x;
    double lat = pts[0].y;

    S2Point s2Point = S2LatLng.fromDegrees(lat, lon).toPoint();
    S2Builder builder = new S2Builder.Builder().build();
    S2PointVectorLayer layer = new S2PointVectorLayer();
    builder.startLayer(layer);
    builder.addPoint(s2Point);

    S2Error error = new S2Error();
    if (!builder.build(error)) {
      throw new IOException("Failed to build S2 point layer: " + error.text());
    }
    List<S2Point> points = layer.getPointVector();
    if (points.isEmpty()) {
      return new SinglePointGeography();
    }
    return new SinglePointGeography(points.get(0));
  }

  private static Geography mPointToGeog(Geometry geom) throws IOException {
    Coordinate[] pts = geom.getCoordinates();
    if (pts.length == 0 || Double.isNaN(pts[0].x) || Double.isNaN(pts[0].y)) {
      return new PointGeography();
    }
    List<S2Point> points = new ArrayList<>(pts.length);
    // Build via S2Builder + S2PointVectorLayer
    S2Builder builder = new S2Builder.Builder().build();
    S2PointVectorLayer layer = new S2PointVectorLayer();
    // must call build() before reading out the points
    S2Error error = new S2Error();
    for (int i = 0; i < pts.length; i++) {
      double lon = pts[i].x;
      double lat = pts[i].y;
      S2Point s2Point = S2LatLng.fromDegrees(lat, lon).toPoint();

      builder.startLayer(layer);
      builder.addPoint(s2Point);

      if (!builder.build(error)) {
        throw new IOException("Failed to build S2 point layer: " + error.text());
      }
    }
    for (int i = 0; i < layer.getPointVector().size(); i++) {
      // Extract the resulting points
      points.add(layer.getPointVector().get(i));
    }
    return new PointGeography(points);
  }

  private static Geography lineToGeog(Geometry geom) throws IOException {
    LineString ls = (LineString) geom;

    // Build S2 points
    List<S2Point> pts = toS2Points(ls.getCoordinates());
    if (pts.size() < 2) {
      // empty or degenerate → empty single polyline
      SinglePolylineGeography empty = new SinglePolylineGeography();
      empty.setSRID(geom.getSRID());
      return empty;
    }

    S2Builder builder = new S2Builder.Builder().build();
    S2PolylineLayer layer = new S2PolylineLayer();
    builder.startLayer(layer);

    builder.addPolyline(new S2Polyline(pts));

    S2Error error = new S2Error();
    if (!builder.build(error)) {
      throw new IOException("Failed to build S2 polyline: " + error.text());
    }
    S2Polyline s2poly = layer.getPolyline();
    return new SinglePolylineGeography(s2poly);
  }

  private static Geography mLineToGeog(Geometry geom) throws IOException {
    MultiLineString mls = (MultiLineString) geom;
    List<S2Polyline> features = new ArrayList<>();
    for (int i = 0; i < mls.getNumGeometries(); i++) {
      LineString ls = (LineString) mls.getGeometryN(i);
      List<S2Point> pts = toS2Points(ls.getCoordinates());
      if (pts.size() >= 2) {
        S2Builder builder = new S2Builder.Builder().build();
        S2PolylineLayer layer = new S2PolylineLayer();
        builder.startLayer(layer);
        builder.addPolyline(new S2Polyline(pts));
        S2Error error = new S2Error();
        if (!builder.build(error)) {
          throw new IOException("Failed to build S2 polyline: " + error.text());
        }
        features.add(layer.getPolyline());
      }
      // Empty/degenerate parts are skipped
    }
    return new PolylineGeography(features);
  }

  private static Geography polyToGeog(Geometry geom) throws IOException {
    // Build S2 loops: exterior + holes
    Polygon poly = (Polygon) geom;
    // Construct S2 polygon (parity handles holes automatically)
    S2Polygon s2poly = toS2Polygon(poly);
    PolygonGeography g = new PolygonGeography(s2poly);
    g.setSRID(geom.getSRID());
    return g;
  }

  private static Geography mPolyToGeog(Geometry geom) throws IOException {
    final List<S2Polygon> polys = new ArrayList<>();

    MultiPolygon mp = (MultiPolygon) geom;
    for (int i = 0; i < mp.getNumGeometries(); i++) {
      Polygon p = (Polygon) mp.getGeometryN(i);
      S2Polygon s2 = toS2Polygon(p);
      if (s2 != null && !s2.isEmpty()) polys.add(s2);
    }

    MultiPolygonGeography out =
        new MultiPolygonGeography(
            Geography.GeographyKind.MULTIPOLYGON, Collections.unmodifiableList(polys));
    out.setSRID(geom.getSRID());
    return out;
  }

  private static Geography geomCollToGeog(Geometry geom) throws IOException {
    if (!(geom instanceof GeometryCollection)) {
      throw new IllegalArgumentException(
          "geomCollToGeog expects GeometryCollection, got " + geom.getGeometryType());
    }
    GeographyCollection coll = new GeographyCollection();

    GeometryCollection gc = (GeometryCollection) geom;
    for (int i = 0; i < gc.getNumGeometries(); i++) {
      Geometry g = gc.getGeometryN(i);
      Geography sub = null;

      if (g instanceof Point) {
        sub = pointToGeog((Point) g);
      } else if (g instanceof MultiPoint) {
        MultiPoint mp = (MultiPoint) g;
        for (int k = 0; k < mp.getNumGeometries(); k++) {
          coll.features.add(pointToGeog((Point) mp.getGeometryN(k)));
        }
        continue;
      } else if (g instanceof LineString) {
        sub = lineToGeog(g);
      } else if (g instanceof MultiLineString) {
        sub = mLineToGeog(g);
      } else if (g instanceof Polygon) {
        sub = polyToGeog(g);
      } else if (g instanceof MultiPolygon) {
        sub = mPolyToGeog(g);
      } else if (g instanceof GeometryCollection) {
        sub = geomCollToGeog(g);
      }
      if (sub != null) coll.features.add(sub);
    }

    coll.setSRID(geom.getSRID());
    return coll;
  }

  /** Convert JTS coordinates to S2 points; drops NaNs and consecutive duplicates. */
  private static List<S2Point> toS2Points(Coordinate[] coords) throws IOException {
    List<S2Point> points = new ArrayList<>(coords.length);
    for (int i = 0; i < coords.length; i++) {
      double lon = coords[i].x;
      double lat = coords[i].y;
      S2Point s2Point = S2LatLng.fromDegrees(lat, lon).toPoint();
      points.add(s2Point);
    }
    return points;
  }

  /** Convert a JTS LinearRing to a normalized S2Loop. Returns null if < 3 distinct vertices. */
  private static S2Loop toS2Loop(LinearRing ring) {
    Coordinate[] coords = ring.getCoordinates();
    if (coords == null || coords.length < 4) { // JTS rings usually have first==last
      return null;
    }

    // Detect if ring is explicitly closed (first == last)
    int end = coords.length;
    if (coords[0].equals2D(coords[coords.length - 1])) {
      end = coords.length - 1; // skip duplicate closing vertex
    }

    List<S2Point> pts = new ArrayList<>(end);
    Double prevLon = null, prevLat = null;

    for (int i = 0; i < end; i++) {
      double lon = coords[i].x; // JTS: x=lon, y=lat
      double lat = coords[i].y;
      if (Double.isNaN(lon) || Double.isNaN(lat)) continue;

      // Skip consecutive duplicates
      if (prevLon != null && lon == prevLon && lat == prevLat) continue;

      pts.add(S2LatLng.fromDegrees(lat, lon).toPoint());
      prevLon = lon;
      prevLat = lat;
    }

    if (pts.size() < 3) return null;

    S2Loop loop = new S2Loop(pts);
    loop.normalize(); // ensure area <= 2π; orientation consistent for S2
    return loop;
  }

  private static S2Polygon toS2Polygon(Polygon poly) throws IOException {
    List<S2Loop> loops = new ArrayList<>();
    S2Loop shell = toS2Loop((LinearRing) poly.getExteriorRing());
    if (shell != null) loops.add(shell);
    for (int i = 0; i < poly.getNumInteriorRing(); i++) {
      S2Loop hole = toS2Loop((LinearRing) poly.getInteriorRingN(i));
      if (hole != null) loops.add(hole);
    }
    if (loops.isEmpty()) return null;
    // Now feed those loops into S2Builder + S2PolygonLayer:
    S2Builder builder = new S2Builder.Builder().build();
    S2PolygonLayer polyLayer = new S2PolygonLayer();
    builder.startLayer(polyLayer);
    // add shell + holes
    for (S2Loop loop : loops) {
      builder.addLoop(loop);
    }
    // build
    S2Error error = new S2Error();
    if (!builder.build(error)) {
      throw new IOException("S2Builder failed: " + error.text());
    }

    // extract the stitched polygon
    return polyLayer.getPolygon(); // even–odd handles holes
  }
}
