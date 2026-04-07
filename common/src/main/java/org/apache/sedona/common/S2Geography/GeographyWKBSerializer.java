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

import java.io.IOException;
import java.util.Arrays;
import org.locationtech.jts.io.ByteOrderValues;

/**
 * Serializer for Geography objects using WKB as the primary format. Supports backward-compatible
 * reading of legacy S2-native serialized data.
 *
 * <p>Format discrimination (first byte):
 *
 * <ul>
 *   <li>{@code 0xFF} — WKB format (new): [0xFF][4-byte SRID big-endian][WKB payload]
 *   <li>{@code 1-10} — S2-native format (legacy): delegates to {@link GeographySerializer}
 * </ul>
 */
public class GeographyWKBSerializer {

  /** Version marker for the WKB format. No GeographyKind has value 255. */
  private static final int FORMAT_WKB = 0xFF;

  /** Header size: 1 byte version + 4 bytes SRID. */
  private static final int HEADER_SIZE = 5;

  /**
   * Serialize a Geography to bytes using WKB format.
   *
   * @param geog the Geography to serialize
   * @return byte array with format: [0xFF][SRID 4 bytes big-endian][WKB payload]
   */
  public static byte[] serialize(Geography geog) throws IOException {
    byte[] wkb;
    if (geog instanceof WKBGeography) {
      wkb = ((WKBGeography) geog).getWKBBytes();
    } else {
      WKBWriter writer = new WKBWriter(2, ByteOrderValues.BIG_ENDIAN, false);
      wkb = writer.write(geog);
    }

    byte[] result = new byte[HEADER_SIZE + wkb.length];
    result[0] = (byte) FORMAT_WKB;
    int srid = geog.getSRID();
    result[1] = (byte) (srid >> 24);
    result[2] = (byte) (srid >> 16);
    result[3] = (byte) (srid >> 8);
    result[4] = (byte) srid;
    System.arraycopy(wkb, 0, result, HEADER_SIZE, wkb.length);
    return result;
  }

  /**
   * Deserialize bytes to a Geography. Supports both WKB format (0xFF prefix) and legacy S2-native
   * format (GeographyKind prefix 1-10).
   *
   * @param buffer the byte array to deserialize
   * @return the deserialized Geography
   */
  public static Geography deserialize(byte[] buffer) throws IOException {
    if ((buffer[0] & 0xFF) == FORMAT_WKB) {
      int srid =
          ((buffer[1] & 0xFF) << 24)
              | ((buffer[2] & 0xFF) << 16)
              | ((buffer[3] & 0xFF) << 8)
              | (buffer[4] & 0xFF);
      byte[] wkb = Arrays.copyOfRange(buffer, HEADER_SIZE, buffer.length);
      return WKBGeography.fromWKB(wkb, srid);
    }
    // Legacy S2-native format: first byte is GeographyKind (1-10)
    return GeographySerializer.deserialize(buffer);
  }
}
