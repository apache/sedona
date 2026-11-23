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
package org.apache.spark.sql.sedona_sql.types;

/**
 * Walks the WKB of a geography that a writer is about to store, without an object for it. A writer
 * calls this for every value, so it allocates nothing.
 */
public final class GeographyWkbScanner {
  /**
   * Set in the result of {@link #scan} for WKB with a type word that is not an ISO one: one with
   * the flags of EWKB for a Z coordinate, an M coordinate or an SRID.
   */
  public static final int NOT_ISO = 1 << 16;

  private GeographyWkbScanner() {}

  /**
   * Walks the WKB that {@code wkb} holds from {@code offset} to its end.
   *
   * @param requireBounds whether a longitude outside of -180 to 180 or a latitude outside of -90 to
   *     90 fails the walk. The coordinates of an empty point are not numbers and pass.
   * @param column the name of the column of the value, for the message of a failure
   * @return the ISO type code of the geography, such as 1001 for a point with a Z coordinate, with
   *     {@link #NOT_ISO} set if a type word of the WKB is not an ISO one
   */
  public static int scan(byte[] wkb, int offset, boolean requireBounds, String column) {
    int end = wkb.length;
    int position = offset;
    int first = 0;
    boolean iso = true;
    long pending = 1;
    while (pending-- > 0) {
      if (end - position < 5) throw invalid(position - offset);
      byte endian = wkb[position];
      if (endian != 0 && endian != 1) throw invalid(position - offset);
      boolean little = endian == 1;
      int code = readInt(wkb, position + 1, little);
      int plain = code & 0x1fffffff;
      int base = plain % 1000;
      int layout = plain / 1000;
      if (base < 1 || base > 7 || layout > 3) throw invalid(position - offset);
      position += 5;
      boolean z = (code & 0x80000000) != 0 || layout == 1 || layout == 3;
      boolean m = (code & 0x40000000) != 0 || layout == 2 || layout == 3;
      int isoCode = base + (z ? 1000 : 0) + (m ? 2000 : 0);
      if (code != isoCode) iso = false;
      if ((code & 0x20000000) != 0) {
        if (end - position < 4) throw invalid(position - offset);
        position += 4;
      }
      if (first == 0) first = isoCode;
      int dimensions = 2 + (z ? 1 : 0) + (m ? 1 : 0);
      switch (base) {
        case 1:
          position = points(wkb, offset, position, 1, dimensions, little, requireBounds, column);
          break;
        case 2:
          {
            int count = count(wkb, offset, position, little);
            position =
                points(wkb, offset, position + 4, count, dimensions, little, requireBounds, column);
            break;
          }
        case 3:
          {
            int rings = count(wkb, offset, position, little);
            position += 4;
            for (int i = 0; i < rings; i++) {
              int count = count(wkb, offset, position, little);
              position =
                  points(
                      wkb, offset, position + 4, count, dimensions, little, requireBounds, column);
            }
            break;
          }
        default:
          pending += count(wkb, offset, position, little);
          position += 4;
      }
    }
    return iso ? first : first | NOT_ISO;
  }

  private static int points(
      byte[] wkb,
      int offset,
      int position,
      int count,
      int dimensions,
      boolean little,
      boolean requireBounds,
      String column) {
    int width = dimensions * 8;
    if ((long) count * width > wkb.length - position) throw invalid(position - offset);
    int end = position + count * width;
    if (requireBounds) {
      for (int at = position; at < end; at += width) {
        double longitude = Double.longBitsToDouble(readLong(wkb, at, little));
        double latitude = Double.longBitsToDouble(readLong(wkb, at + 8, little));
        // The coordinates of an empty point are not numbers, which are in no range
        if (longitude < -180 || longitude > 180 || latitude < -90 || latitude > 90) {
          throw new IllegalArgumentException(
              "Column '"
                  + column
                  + "' has a geography with longitude "
                  + number(longitude)
                  + " and latitude "
                  + number(latitude)
                  + ". The GEOGRAPHY logical type of the Parquet format takes longitudes from -180"
                  + " to 180 and latitudes from -90 to 90");
        }
      }
    }
    return end;
  }

  private static String number(double value) {
    return value == Math.rint(value) && Math.abs(value) < 1e15
        ? Long.toString((long) value)
        : Double.toString(value);
  }

  private static int count(byte[] wkb, int offset, int position, boolean little) {
    if (wkb.length - position < 4) throw invalid(position - offset);
    int count = readInt(wkb, position, little);
    if (count < 0) throw invalid(position - offset);
    return count;
  }

  private static int readInt(byte[] bytes, int at, boolean little) {
    int b0 = bytes[at] & 0xff;
    int b1 = bytes[at + 1] & 0xff;
    int b2 = bytes[at + 2] & 0xff;
    int b3 = bytes[at + 3] & 0xff;
    return little ? b0 | b1 << 8 | b2 << 16 | b3 << 24 : b0 << 24 | b1 << 16 | b2 << 8 | b3;
  }

  private static long readLong(byte[] bytes, int at, boolean little) {
    long low = readInt(bytes, little ? at : at + 4, little) & 0xffffffffL;
    long high = readInt(bytes, little ? at + 4 : at, little) & 0xffffffffL;
    return high << 32 | low;
  }

  private static IllegalArgumentException invalid(int position) {
    return new IllegalArgumentException("Invalid WKB framing at byte " + position);
  }
}
