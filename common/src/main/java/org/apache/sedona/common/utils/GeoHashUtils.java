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

import java.util.Arrays;

/**
 * Shared constants and utilities for geohash encoding/decoding.
 *
 * <p>Centralizes the base32 alphabet and decode lookup table previously duplicated across {@link
 * GeoHashDecoder}, {@link PointGeoHashEncoder}, and {@link GeoHashNeighbor}.
 */
public class GeoHashUtils {

  /** The base32 alphabet used by geohash encoding (Gustavo Niemeyer's specification). */
  public static final String BASE32 = "0123456789bcdefghjkmnpqrstuvwxyz";

  /**
   * Base32 character array for index-based lookup. Package-private to prevent external mutation.
   */
  static final char[] BASE32_CHARS = BASE32.toCharArray();

  /**
   * Bit masks for extracting 5-bit groups: {16, 8, 4, 2, 1}. Package-private to prevent external
   * mutation.
   */
  static final int[] BITS = new int[] {16, 8, 4, 2, 1};

  /** Number of bits per base32 character. */
  public static final int BITS_PER_CHAR = 5;

  /**
   * Reverse lookup array: maps a character (as int) to its base32 index. Invalid characters map to
   * -1. Package-private to prevent external mutation.
   */
  static final int[] DECODE_ARRAY = new int['z' + 1];

  static {
    Arrays.fill(DECODE_ARRAY, -1);
    for (int i = 0; i < BASE32_CHARS.length; i++) {
      DECODE_ARRAY[BASE32_CHARS[i]] = i;
    }
  }

  /**
   * Decodes a single base32 character to its integer value (0-31).
   *
   * @param c the character to decode
   * @return the integer value, or -1 if the character is not a valid base32 character
   */
  public static int decodeChar(char c) {
    if (c >= DECODE_ARRAY.length) {
      return -1;
    }
    return DECODE_ARRAY[c];
  }

  private GeoHashUtils() {}
}
