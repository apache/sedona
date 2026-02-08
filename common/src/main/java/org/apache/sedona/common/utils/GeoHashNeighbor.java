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

/**
 * Utility class for computing geohash neighbors.
 *
 * <p>The neighbor algorithm is ported from the geohash-java library by Silvio Heuberger (Apache 2.0
 * License). It works by de-interleaving the geohash bits into separate latitude/longitude
 * components, incrementing/decrementing the appropriate component, masking for wrap-around, and
 * re-interleaving back to a geohash string.
 *
 * @see <a href="https://github.com/kungfoo/geohash-java">geohash-java</a>
 */
public class GeoHashNeighbor {

  private static final int MAX_BIT_PRECISION = 64;
  private static final long FIRST_BIT_FLAGGED = 0x8000000000000000L;

  // Shared base32 constants from GeoHashUtils
  private static final char[] base32 = GeoHashUtils.BASE32_CHARS;
  private static final int[] decodeArray = GeoHashUtils.DECODE_ARRAY;

  /**
   * Returns all 8 neighbors of the given geohash in the order: [N, NE, E, SE, S, SW, W, NW].
   *
   * @param geohash the geohash string
   * @return array of 8 neighbor geohash strings, or null if input is null
   */
  public static String[] getNeighbors(String geohash) {
    if (geohash == null) {
      return null;
    }

    long[] parsed = parseGeohash(geohash);
    long bits = parsed[0];
    int significantBits = (int) parsed[1];

    // Ported from geohash-java: GeoHash.getAdjacent()
    String northern = neighborLat(bits, significantBits, 1);
    String eastern = neighborLon(bits, significantBits, 1);
    String southern = neighborLat(bits, significantBits, -1);
    String western = neighborLon(bits, significantBits, -1);

    // Diagonal neighbors: compose two cardinal moves
    long[] northBits = parseGeohash(northern);
    long[] southBits = parseGeohash(southern);

    return new String[] {
      northern,
      neighborLon(northBits[0], significantBits, 1), // NE
      eastern,
      neighborLon(southBits[0], significantBits, 1), // SE
      southern,
      neighborLon(southBits[0], significantBits, -1), // SW
      western,
      neighborLon(northBits[0], significantBits, -1) // NW
    };
  }

  /**
   * Returns the neighbor of the given geohash in the specified direction.
   *
   * @param geohash the geohash string
   * @param direction compass direction: "n", "ne", "e", "se", "s", "sw", "w", "nw"
   *     (case-insensitive)
   * @return the neighbor geohash string, or null if input geohash is null
   */
  public static String getNeighbor(String geohash, String direction) {
    if (geohash == null) {
      return null;
    }
    if (direction == null) {
      throw new IllegalArgumentException(
          "Direction cannot be null. Valid values: n, ne, e, se, s, sw, w, nw");
    }

    long[] parsed = parseGeohash(geohash);
    long bits = parsed[0];
    int significantBits = (int) parsed[1];

    switch (direction.toLowerCase()) {
      case "n":
        return neighborLat(bits, significantBits, 1);
      case "s":
        return neighborLat(bits, significantBits, -1);
      case "e":
        return neighborLon(bits, significantBits, 1);
      case "w":
        return neighborLon(bits, significantBits, -1);
      case "ne":
        {
          long[] n = parseGeohash(neighborLat(bits, significantBits, 1));
          return neighborLon(n[0], significantBits, 1);
        }
      case "se":
        {
          long[] s = parseGeohash(neighborLat(bits, significantBits, -1));
          return neighborLon(s[0], significantBits, 1);
        }
      case "sw":
        {
          long[] s = parseGeohash(neighborLat(bits, significantBits, -1));
          return neighborLon(s[0], significantBits, -1);
        }
      case "nw":
        {
          long[] n = parseGeohash(neighborLat(bits, significantBits, 1));
          return neighborLon(n[0], significantBits, -1);
        }
      default:
        throw new IllegalArgumentException(
            "Invalid direction: '" + direction + "'. Valid values: n, ne, e, se, s, sw, w, nw");
    }
  }

  /**
   * Parses a geohash string into [bits, significantBits]. Based on geohash-java
   * GeoHash.fromGeohashString().
   */
  private static long[] parseGeohash(String geohash) {
    if (geohash.isEmpty()) {
      throw new IllegalArgumentException("Geohash string cannot be empty");
    }
    if (geohash.length() * GeoHashUtils.BITS_PER_CHAR > MAX_BIT_PRECISION) {
      throw new IllegalArgumentException(
          "Geohash '"
              + geohash
              + "' is too long (max "
              + (MAX_BIT_PRECISION / GeoHashUtils.BITS_PER_CHAR)
              + " characters)");
    }
    long bits = 0;
    int significantBits = 0;
    for (int i = 0; i < geohash.length(); i++) {
      char c = geohash.charAt(i);
      int cd;
      if (c >= decodeArray.length || (cd = decodeArray[c]) < 0) {
        throw new IllegalArgumentException(
            "Invalid character '" + c + "' in geohash '" + geohash + "'");
      }
      for (int j = 0; j < GeoHashUtils.BITS_PER_CHAR; j++) {
        significantBits++;
        bits <<= 1;
        if ((cd & (16 >> j)) != 0) {
          bits |= 0x1;
        }
      }
    }
    bits <<= (MAX_BIT_PRECISION - significantBits);
    return new long[] {bits, significantBits};
  }

  // Ported from geohash-java: GeoHash.getNorthernNeighbour() / getSouthernNeighbour()
  private static String neighborLat(long bits, int significantBits, int delta) {
    long[] latBits = getRightAlignedLatitudeBits(bits, significantBits);
    long[] lonBits = getRightAlignedLongitudeBits(bits, significantBits);
    latBits[0] += delta;
    latBits[0] = maskLastNBits(latBits[0], latBits[1]);
    return recombineLatLonBitsToBase32(latBits, lonBits);
  }

  // Ported from geohash-java: GeoHash.getEasternNeighbour() / getWesternNeighbour()
  private static String neighborLon(long bits, int significantBits, int delta) {
    long[] latBits = getRightAlignedLatitudeBits(bits, significantBits);
    long[] lonBits = getRightAlignedLongitudeBits(bits, significantBits);
    lonBits[0] += delta;
    lonBits[0] = maskLastNBits(lonBits[0], lonBits[1]);
    return recombineLatLonBitsToBase32(latBits, lonBits);
  }

  // Ported from geohash-java: GeoHash.getRightAlignedLatitudeBits()
  private static long[] getRightAlignedLatitudeBits(long bits, int significantBits) {
    long copyOfBits = bits << 1;
    int[] numBits = getNumberOfLatLonBits(significantBits);
    long value = extractEverySecondBit(copyOfBits, numBits[0]);
    return new long[] {value, numBits[0]};
  }

  // Ported from geohash-java: GeoHash.getRightAlignedLongitudeBits()
  private static long[] getRightAlignedLongitudeBits(long bits, int significantBits) {
    int[] numBits = getNumberOfLatLonBits(significantBits);
    long value = extractEverySecondBit(bits, numBits[1]);
    return new long[] {value, numBits[1]};
  }

  // Copied from geohash-java: GeoHash.extractEverySecondBit()
  private static long extractEverySecondBit(long copyOfBits, int numberOfBits) {
    long value = 0;
    for (int i = 0; i < numberOfBits; i++) {
      if ((copyOfBits & FIRST_BIT_FLAGGED) == FIRST_BIT_FLAGGED) {
        value |= 0x1;
      }
      value <<= 1;
      copyOfBits <<= 2;
    }
    value >>>= 1;
    return value;
  }

  // Copied from geohash-java: GeoHash.getNumberOfLatLonBits()
  private static int[] getNumberOfLatLonBits(int significantBits) {
    if (significantBits % 2 == 0) {
      return new int[] {significantBits / 2, significantBits / 2};
    } else {
      return new int[] {significantBits / 2, significantBits / 2 + 1};
    }
  }

  // Copied from geohash-java: GeoHash.maskLastNBits()
  private static long maskLastNBits(long value, long n) {
    long mask = 0xFFFFFFFFFFFFFFFFL;
    mask >>>= (MAX_BIT_PRECISION - n);
    return value & mask;
  }

  /**
   * Re-interleaves lat/lon bits and converts to base32 string. Simplified from geohash-java's
   * GeoHash.recombineLatLonBitsToHash() — we only need the base32 output, not the full GeoHash
   * object with bounding box.
   */
  private static String recombineLatLonBitsToBase32(long[] latBits, long[] lonBits) {
    int significantBits = (int) (latBits[1] + lonBits[1]);
    long lat = latBits[0] << (MAX_BIT_PRECISION - latBits[1]);
    long lon = lonBits[0] << (MAX_BIT_PRECISION - lonBits[1]);

    long bits = 0;
    boolean isEvenBit = false;
    for (int i = 0; i < significantBits; i++) {
      bits <<= 1;
      if (isEvenBit) {
        if ((lat & FIRST_BIT_FLAGGED) == FIRST_BIT_FLAGGED) {
          bits |= 0x1;
        }
        lat <<= 1;
      } else {
        if ((lon & FIRST_BIT_FLAGGED) == FIRST_BIT_FLAGGED) {
          bits |= 0x1;
        }
        lon <<= 1;
      }
      isEvenBit = !isEvenBit;
    }
    bits <<= (MAX_BIT_PRECISION - significantBits);

    // Ported from geohash-java: GeoHash.toBase32()
    StringBuilder buf = new StringBuilder();
    long firstFiveBitsMask = 0xF800000000000000L;
    long bitsCopy = bits;
    int numChars = significantBits / GeoHashUtils.BITS_PER_CHAR;
    for (int i = 0; i < numChars; i++) {
      int pointer = (int) ((bitsCopy & firstFiveBitsMask) >>> 59);
      buf.append(base32[pointer]);
      bitsCopy <<= 5;
    }
    return buf.toString();
  }
}
