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

import com.google.common.geometry.PrimitiveArrays;
import com.google.common.geometry.S2Coder;
import com.google.common.geometry.S2Point;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

public abstract class CountingPointVectorCoder implements S2Coder<S2Point.Shape> {
  public static final CountingPointVectorCoder INSTANCE =
      new CountingPointVectorCoder() {
        @Override
        public S2Point.Shape decode(PrimitiveArrays.Bytes data, PrimitiveArrays.Cursor cursor) {
          return null;
        }
      };

  private CountingPointVectorCoder() {}

  @Override
  public void encode(S2Point.Shape shape, OutputStream out) throws IOException {
    // varint count
    writeVarInt(out, shape.numEdges());
    // raw doubles
    for (int i = 0; i < shape.numEdges(); i++) {
      S2Point p = shape.get(i);
      writeDouble(out, p.getX());
      writeDouble(out, p.getY());
      writeDouble(out, p.getZ());
    }
  }

  // unused by us:
  public S2Point.Shape decode(byte[] data, int offset, int length) {
    throw new UnsupportedOperationException();
  }

  public List<S2Point> decode(InputStream in) throws IOException {
    long n = readVarInt(in);
    List<S2Point> pts = new ArrayList<>((int) n);
    for (int i = 0; i < n; i++) {
      double x = readDouble(in);
      double y = readDouble(in);
      double z = readDouble(in);
      pts.add(new S2Point(x, y, z));
    }
    return pts;
  }

  private static void writeVarInt(OutputStream out, long v) throws IOException {
    while (true) {
      int bits = (int) (v & 0x7F); // grab low 7 bits
      v >>>= 7;
      if (v != 0) out.write(bits | 0x80); // yes: set continuation bit
      else {
        out.write(bits);
        break;
      }
    }
  }
  // [varint(count=N)] [point_0.x][point_0.y][point_0.z] … [point_{N-1}.x][…]
  static long readVarInt(InputStream in) throws IOException {
    long res = 0;
    int shift = 0;
    while (true) {
      int b = in.read();
      if (b < 0) throw new IOException("EOF varint");
      res |= (long) (b & 0x7F) << shift;
      if ((b & 0x80) == 0) break;
      shift += 7;
    }
    return res;
  }

  private static void writeDouble(OutputStream out, double x) throws IOException {
    long bits = Double.doubleToLongBits(x);
    for (int i = 0; i < 8; i++) out.write((int) (bits >> (8 * i)) & 0xFF);
  }

  private static double readDouble(InputStream in) throws IOException {
    long bits = 0;
    for (int i = 0; i < 8; i++) {
      int b = in.read();
      if (b < 0) throw new IOException("EOF double");
      bits |= (long) (b & 0xFF) << (8 * i);
    }
    return Double.longBitsToDouble(bits);
  }
}
