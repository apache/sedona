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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Polygon;

/**
 * Represents a Bing Maps tile. Bing Maps uses a quadtree-based tiling system where tiles are
 * identified by (x, y, zoomLevel) coordinates or equivalently by a quadkey string.
 *
 * <p>The implementation is based on the Bing Maps Tile System specification:
 * https://docs.microsoft.com/en-us/bingmaps/articles/bing-maps-tile-system
 *
 * <p>Ported from https://github.com/wwbrannon/bing-tile-hive (Apache-2.0 license), which itself is
 * a port of Presto's Bing Tile implementation. Adapted for JTS geometry.
 */
public final class BingTile {

  public static final int MAX_ZOOM_LEVEL = 23;
  private static final int TILE_PIXELS = 256;
  private static final double MAX_LATITUDE = 85.05112878;
  private static final double MIN_LATITUDE = -85.05112878;
  private static final double MAX_LONGITUDE = 180;
  private static final double MIN_LONGITUDE = -180;

  private static final int OPTIMIZED_TILING_MIN_ZOOM_LEVEL = 10;

  private static final String LATITUDE_OUT_OF_RANGE =
      "Latitude must be between " + MIN_LATITUDE + " and " + MAX_LATITUDE;
  private static final String LONGITUDE_OUT_OF_RANGE =
      "Longitude must be between " + MIN_LONGITUDE + " and " + MAX_LONGITUDE;
  private static final String QUAD_KEY_EMPTY = "QuadKey must not be empty string";
  private static final String QUAD_KEY_TOO_LONG =
      "QuadKey must be " + MAX_ZOOM_LEVEL + " characters or less";
  private static final String ZOOM_LEVEL_TOO_SMALL = "Zoom level must be > 0";
  private static final String ZOOM_LEVEL_TOO_LARGE = "Zoom level must be <= " + MAX_ZOOM_LEVEL;

  private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();

  private final int x;
  private final int y;
  private final int zoomLevel;

  // =========================================================================
  // Constructors
  // =========================================================================

  private BingTile(int x, int y, int zoomLevel) {
    checkZoomLevel(zoomLevel);
    checkCoordinate(x, zoomLevel);
    checkCoordinate(y, zoomLevel);
    this.x = x;
    this.y = y;
    this.zoomLevel = zoomLevel;
  }

  // =========================================================================
  // Validation methods
  // =========================================================================

  private static void checkCondition(boolean condition, String formatString, Object... args) {
    if (!condition) {
      throw new IllegalArgumentException(String.format(formatString, args));
    }
  }

  private static void checkZoomLevel(long zoomLevel) {
    checkCondition(zoomLevel > 0, ZOOM_LEVEL_TOO_SMALL);
    checkCondition(zoomLevel <= MAX_ZOOM_LEVEL, ZOOM_LEVEL_TOO_LARGE);
  }

  private static void checkCoordinate(long coordinate, long zoomLevel) {
    checkCondition(
        coordinate >= 0 && coordinate < (1 << zoomLevel),
        "XY coordinates for a Bing tile at zoom level %s must be within [0, %s) range",
        zoomLevel,
        1 << zoomLevel);
  }

  private static void checkQuadKey(String quadkey) {
    checkCondition(quadkey.length() > 0, QUAD_KEY_EMPTY);
    checkCondition(quadkey.length() <= MAX_ZOOM_LEVEL, QUAD_KEY_TOO_LONG);
  }

  private static void checkLatitude(double latitude) {
    checkCondition(latitude >= MIN_LATITUDE && latitude <= MAX_LATITUDE, LATITUDE_OUT_OF_RANGE);
  }

  private static void checkLongitude(double longitude) {
    checkCondition(
        longitude >= MIN_LONGITUDE && longitude <= MAX_LONGITUDE, LONGITUDE_OUT_OF_RANGE);
  }

  // =========================================================================
  // Utility functions for converting to/from latitude/longitude
  // =========================================================================

  private static long mapSize(int zoomLevel) {
    return 256L << zoomLevel;
  }

  private static double clip(double n, double minValue, double maxValue) {
    return Math.min(Math.max(n, minValue), maxValue);
  }

  private static int axisToCoordinates(double axis, long mapSize) {
    int tileAxis = (int) clip(axis * mapSize, 0, mapSize - 1);
    return tileAxis / TILE_PIXELS;
  }

  private static int longitudeToTileX(double longitude, long mapSize) {
    double x = (longitude + 180) / 360;
    return axisToCoordinates(x, mapSize);
  }

  private static int latitudeToTileY(double latitude, long mapSize) {
    double sinLatitude = Math.sin(latitude * Math.PI / 180);
    double y = 0.5 - Math.log((1 + sinLatitude) / (1 - sinLatitude)) / (4 * Math.PI);
    return axisToCoordinates(y, mapSize);
  }

  /**
   * Converts tile XY coordinates to the upper-left corner latitude/longitude of the tile.
   *
   * @return a Coordinate with x=longitude, y=latitude
   */
  private static Coordinate tileXYToLatitudeLongitude(int tileX, int tileY, int zoomLevel) {
    long mapSize = mapSize(zoomLevel);
    double x = (clip(tileX * TILE_PIXELS, 0, mapSize) / mapSize) - 0.5;
    double y = 0.5 - (clip(tileY * TILE_PIXELS, 0, mapSize) / mapSize);
    double lat = 90 - 360 * Math.atan(Math.exp(-y * 2 * Math.PI)) / Math.PI;
    double lon = 360 * x;
    return new Coordinate(lon, lat);
  }

  // =========================================================================
  // Factory methods
  // =========================================================================

  /**
   * Creates a BingTile from XY coordinates and zoom level.
   *
   * @param x the tile X coordinate
   * @param y the tile Y coordinate
   * @param zoomLevel the zoom level (1 to 23)
   * @return a new BingTile
   */
  public static BingTile fromCoordinates(int x, int y, int zoomLevel) {
    return new BingTile(x, y, zoomLevel);
  }

  /**
   * Creates a BingTile from a latitude/longitude point at the given zoom level.
   *
   * @param latitude the latitude (-85.05112878 to 85.05112878)
   * @param longitude the longitude (-180 to 180)
   * @param zoomLevel the zoom level (1 to 23)
   * @return a new BingTile
   */
  public static BingTile fromLatLon(double latitude, double longitude, int zoomLevel) {
    checkLatitude(latitude);
    checkLongitude(longitude);
    checkZoomLevel(zoomLevel);

    long mapSize = mapSize(zoomLevel);
    int tileX = longitudeToTileX(longitude, mapSize);
    int tileY = latitudeToTileY(latitude, mapSize);
    return new BingTile(tileX, tileY, zoomLevel);
  }

  /**
   * Creates a BingTile from a quadkey string.
   *
   * @param quadKey the quadkey string (e.g. "0231")
   * @return a new BingTile
   */
  public static BingTile fromQuadKey(String quadKey) {
    checkQuadKey(quadKey);

    int zoomLevel = quadKey.length();
    checkZoomLevel(zoomLevel);

    int tileX = 0;
    int tileY = 0;

    for (int i = zoomLevel; i > 0; i--) {
      int mask = 1 << (i - 1);
      switch (quadKey.charAt(zoomLevel - i)) {
        case '0':
          break;
        case '1':
          tileX |= mask;
          break;
        case '2':
          tileY |= mask;
          break;
        case '3':
          tileX |= mask;
          tileY |= mask;
          break;
        default:
          throw new IllegalArgumentException("Invalid QuadKey digit: " + quadKey);
      }
    }
    return new BingTile(tileX, tileY, zoomLevel);
  }

  // =========================================================================
  // Accessors
  // =========================================================================

  public int getX() {
    return x;
  }

  public int getY() {
    return y;
  }

  public int getZoomLevel() {
    return zoomLevel;
  }

  // =========================================================================
  // Conversion methods
  // =========================================================================

  /**
   * Converts this tile to its quadkey string representation.
   *
   * @return the quadkey string
   */
  public String toQuadKey() {
    char[] quadKey = new char[this.zoomLevel];
    for (int i = this.zoomLevel; i > 0; i--) {
      char digit = '0';
      int mask = 1 << (i - 1);
      if ((this.x & mask) != 0) {
        digit++;
      }
      if ((this.y & mask) != 0) {
        digit += 2;
      }
      quadKey[this.zoomLevel - i] = digit;
    }
    return String.valueOf(quadKey);
  }

  /**
   * Returns the bounding box envelope of this tile.
   *
   * @return the JTS Envelope
   */
  public Envelope toEnvelope() {
    Coordinate upperLeftCorner = tileXYToLatitudeLongitude(this.x, this.y, this.zoomLevel);
    Coordinate lowerRightCorner = tileXYToLatitudeLongitude(this.x + 1, this.y + 1, this.zoomLevel);

    return new Envelope(
        upperLeftCorner.x, lowerRightCorner.x, lowerRightCorner.y, upperLeftCorner.y);
  }

  /**
   * Returns the polygon geometry of this tile.
   *
   * @return the JTS Polygon
   */
  public Polygon toPolygon() {
    Envelope envelope = toEnvelope();
    Coordinate[] coordinates =
        new Coordinate[] {
          new Coordinate(envelope.getMinX(), envelope.getMinY()),
          new Coordinate(envelope.getMinX(), envelope.getMaxY()),
          new Coordinate(envelope.getMaxX(), envelope.getMaxY()),
          new Coordinate(envelope.getMaxX(), envelope.getMinY()),
          new Coordinate(envelope.getMinX(), envelope.getMinY())
        };
    return GEOMETRY_FACTORY.createPolygon(coordinates);
  }

  // =========================================================================
  // Overridden Object methods
  // =========================================================================

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (other == null || getClass() != other.getClass()) {
      return false;
    }
    BingTile otherTile = (BingTile) other;
    return this.x == otherTile.x && this.y == otherTile.y && this.zoomLevel == otherTile.zoomLevel;
  }

  @Override
  public int hashCode() {
    return Objects.hash(x, y, zoomLevel);
  }

  @Override
  public String toString() {
    return toQuadKey();
  }

  // =========================================================================
  // Spatial query methods
  // =========================================================================

  /**
   * Returns the tiles around a given latitude/longitude at the specified zoom level. Returns the
   * 3x3 neighborhood of tiles centered on the tile containing the point.
   *
   * @param latitude the latitude
   * @param longitude the longitude
   * @param zoomLevel the zoom level
   * @return list of BingTile objects
   */
  public static List<BingTile> tilesAround(double latitude, double longitude, int zoomLevel) {
    checkLatitude(latitude);
    checkLongitude(longitude);
    checkZoomLevel(zoomLevel);

    List<BingTile> ret = new ArrayList<>();
    long mapSize = mapSize(zoomLevel);
    long maxTileIndex = (mapSize / TILE_PIXELS) - 1;

    int tileX = longitudeToTileX(longitude, mapSize);
    int tileY = latitudeToTileY(latitude, mapSize);

    for (int i = -1; i <= 1; i++) {
      for (int j = -1; j <= 1; j++) {
        int newX = tileX + i;
        int newY = tileY + j;
        if (newX >= 0 && newX <= maxTileIndex && newY >= 0 && newY <= maxTileIndex) {
          ret.add(BingTile.fromCoordinates(newX, newY, zoomLevel));
        }
      }
    }
    return ret;
  }

  /**
   * Returns the minimum set of Bing tiles that fully covers a given geometry at the specified zoom
   * level.
   *
   * @param geometry the JTS Geometry to cover
   * @param zoomLevel the zoom level (1 to 23)
   * @return list of BingTile objects covering the geometry
   */
  public static List<BingTile> tilesCovering(Geometry geometry, int zoomLevel) {
    checkZoomLevel(zoomLevel);

    List<BingTile> ret = new ArrayList<>();
    if (geometry.isEmpty()) {
      return ret;
    }

    Envelope envelope = geometry.getEnvelopeInternal();

    checkLatitude(envelope.getMinY());
    checkLatitude(envelope.getMaxY());
    checkLongitude(envelope.getMinX());
    checkLongitude(envelope.getMaxX());

    boolean pointOrRectangle = isPointOrRectangle(geometry, envelope);

    BingTile leftUpperTile = fromLatLon(envelope.getMaxY(), envelope.getMinX(), zoomLevel);
    BingTile rightLowerTile = getTileCoveringLowerRightCorner(envelope, zoomLevel);

    // XY coordinates start at (0,0) in the left upper corner and increase left to right and
    // top to bottom
    long tileCount =
        (long) (rightLowerTile.getX() - leftUpperTile.getX() + 1)
            * (rightLowerTile.getY() - leftUpperTile.getY() + 1);

    checkGeometryToBingTilesLimits(geometry, pointOrRectangle, tileCount);

    if (pointOrRectangle || zoomLevel <= OPTIMIZED_TILING_MIN_ZOOM_LEVEL) {
      // Collect tiles covering the bounding box and check each tile for intersection
      for (int x = leftUpperTile.getX(); x <= rightLowerTile.getX(); x++) {
        for (int y = leftUpperTile.getY(); y <= rightLowerTile.getY(); y++) {
          BingTile tile = BingTile.fromCoordinates(x, y, zoomLevel);
          if (pointOrRectangle || !tileDisjoint(tile, geometry)) {
            ret.add(tile);
          }
        }
      }
    } else {
      // Optimized tiling: identify large tiles fully covered by geometry then expand
      BingTile[] tiles =
          getTilesInBetween(leftUpperTile, rightLowerTile, OPTIMIZED_TILING_MIN_ZOOM_LEVEL);
      for (BingTile tile : tiles) {
        appendIntersectingSubtiles(geometry, zoomLevel, tile, ret);
      }
    }

    return ret;
  }

  // =========================================================================
  // Internal helper methods for tilesCovering
  // =========================================================================

  private static boolean isPointOrRectangle(Geometry geometry, Envelope envelope) {
    if (geometry instanceof org.locationtech.jts.geom.Point) {
      return true;
    }
    if (!(geometry instanceof Polygon)) {
      return false;
    }
    Polygon polygon = (Polygon) geometry;
    if (polygon.getNumInteriorRing() > 0) {
      return false;
    }
    Coordinate[] coords = polygon.getExteriorRing().getCoordinates();
    // A closed rectangle has 5 coordinates (first == last)
    if (coords.length != 5) {
      return false;
    }
    // Check that all corners match the envelope corners
    for (int i = 0; i < 4; i++) {
      Coordinate c = coords[i];
      boolean matchesCorner =
          (c.x == envelope.getMinX() || c.x == envelope.getMaxX())
              && (c.y == envelope.getMinY() || c.y == envelope.getMaxY());
      if (!matchesCorner) {
        return false;
      }
    }
    return true;
  }

  private static void checkGeometryToBingTilesLimits(
      Geometry geometry, boolean pointOrRectangle, long tileCount) {
    if (pointOrRectangle) {
      checkCondition(
          tileCount <= 1_000_000,
          "The number of input tiles is too large (more than 1M) to compute a set of covering Bing tiles.");
    } else {
      checkCondition(
          (int) tileCount == tileCount,
          "The zoom level is too high to compute a set of covering Bing tiles.");

      long pointCount = geometry.getNumPoints();
      long complexity;
      try {
        complexity = Math.multiplyExact(tileCount, pointCount);
      } catch (ArithmeticException e) {
        throw new IllegalArgumentException(
            "The zoom level is too high or the geometry is too complex to compute a set of covering Bing tiles. "
                + "Please use a lower zoom level or convert the geometry to its bounding box using the ST_Envelope function.");
      }
      checkCondition(
          complexity <= 25_000_000,
          "The zoom level is too high or the geometry is too complex to compute a set of covering Bing tiles. "
              + "Please use a lower zoom level or convert the geometry to its bounding box using the ST_Envelope function.");
    }
  }

  private static BingTile getTileCoveringLowerRightCorner(Envelope envelope, int zoomLevel) {
    BingTile tile = fromLatLon(envelope.getMinY(), envelope.getMaxX(), zoomLevel);

    // If the tile covering the lower right corner of the envelope overlaps the envelope only
    // at the border then return a tile shifted to the left and/or up
    int deltaX = 0;
    int deltaY = 0;

    Coordinate upperLeftCorner =
        tileXYToLatitudeLongitude(tile.getX(), tile.getY(), tile.getZoomLevel());
    if (upperLeftCorner.x == envelope.getMaxX()) {
      deltaX = -1;
    }
    if (upperLeftCorner.y == envelope.getMinY()) {
      deltaY = -1;
    }

    if (deltaX == 0 && deltaY == 0) {
      return tile;
    } else {
      return BingTile.fromCoordinates(
          tile.getX() + deltaX, tile.getY() + deltaY, tile.getZoomLevel());
    }
  }

  private static BingTile[] getTilesInBetween(
      BingTile leftUpperTile, BingTile rightLowerTile, int zoomLevel) {
    checkCondition(
        leftUpperTile.getZoomLevel() == rightLowerTile.getZoomLevel(), "Mismatched zoom levels");
    checkCondition(leftUpperTile.getZoomLevel() > zoomLevel, "Tile zoom level too low");

    int divisor = 1 << (leftUpperTile.getZoomLevel() - zoomLevel);

    int minX = (int) Math.floor((double) leftUpperTile.getX() / divisor);
    int maxX = (int) Math.floor((double) rightLowerTile.getX() / divisor);
    int minY = (int) Math.floor((double) leftUpperTile.getY() / divisor);
    int maxY = (int) Math.floor((double) rightLowerTile.getY() / divisor);

    BingTile[] tiles = new BingTile[(maxX - minX + 1) * (maxY - minY + 1)];

    int index = 0;
    for (int x = minX; x <= maxX; x++) {
      for (int y = minY; y <= maxY; y++) {
        tiles[index] = BingTile.fromCoordinates(x, y, OPTIMIZED_TILING_MIN_ZOOM_LEVEL);
        index++;
      }
    }
    return tiles;
  }

  private static boolean tileDisjoint(BingTile tile, Geometry geometry) {
    Polygon tilePolygon = tile.toPolygon();
    return tilePolygon.disjoint(geometry);
  }

  private static boolean geometryContainsTile(Geometry geometry, BingTile tile) {
    Polygon tilePolygon = tile.toPolygon();
    return geometry.contains(tilePolygon);
  }

  private static void appendIntersectingSubtiles(
      Geometry geometry, int zoomLevel, BingTile tile, List<BingTile> result) {
    int tileZoomLevel = tile.getZoomLevel();
    checkCondition(tileZoomLevel <= zoomLevel, "Tile zoom level too high");

    if (tileZoomLevel == zoomLevel) {
      if (!tileDisjoint(tile, geometry)) {
        result.add(tile);
      }
      return;
    }

    if (geometryContainsTile(geometry, tile)) {
      // Tile is fully contained — add all sub-tiles at the target zoom level
      int subTileCount = 1 << (zoomLevel - tileZoomLevel);
      int minX = subTileCount * tile.getX();
      int minY = subTileCount * tile.getY();

      for (int x = minX; x < minX + subTileCount; x++) {
        for (int y = minY; y < minY + subTileCount; y++) {
          result.add(BingTile.fromCoordinates(x, y, zoomLevel));
        }
      }
      return;
    }

    if (tileDisjoint(tile, geometry)) {
      return;
    }

    // Recurse into the 4 children
    int nextZoomLevel = tileZoomLevel + 1;
    int minX = 2 * tile.getX();
    int minY = 2 * tile.getY();

    for (int x = minX; x < minX + 2; x++) {
      for (int y = minY; y < minY + 2; y++) {
        appendIntersectingSubtiles(
            geometry, zoomLevel, BingTile.fromCoordinates(x, y, nextZoomLevel), result);
      }
    }
  }
}
