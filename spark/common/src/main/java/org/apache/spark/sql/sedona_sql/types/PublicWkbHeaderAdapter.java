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

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/** Header-only public EWKB-to-ISO adaptation; Spark still validates and serializes geometry. */
public final class PublicWkbHeaderAdapter {
  public static byte[] toIsoWkb(byte[] source) {
    Cursor sizing = new Cursor(source, null);
    int length = sizing.walk();
    if (!sizing.changed) return source;
    byte[] result = new byte[length];
    new Cursor(source, result).walk();
    return result;
  }

  private static final class Cursor {
    final byte[] source;
    final ByteBuffer input, output;
    int outputSize;
    boolean changed;

    Cursor(byte[] source, byte[] target) {
      this.source = source;
      input = ByteBuffer.wrap(source);
      output = target == null ? null : ByteBuffer.wrap(target);
    }

    IllegalArgumentException invalid() {
      return new IllegalArgumentException("Invalid WKB framing at byte " + input.position());
    }

    int count() {
      int n = input.getInt();
      if (n < 0) throw invalid();
      if (output != null) output.putInt(n);
      outputSize += Integer.BYTES;
      return n;
    }

    void coordinates(long bytes) {
      if (bytes > input.remaining()) throw invalid();
      int n = (int) bytes;
      if (output != null) output.put(source, input.position(), n);
      outputSize += n;
      input.position(input.position() + n);
    }

    int walk() {
      try {
        long pending = 1;
        while (pending-- > 0) {
          byte endian = input.get();
          if (endian != 0 && endian != 1) throw invalid();
          ByteOrder order = endian == 1 ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN;
          input.order(order);
          int code = input.getInt(), plain = code & 0x1fffffff;
          int base = plain % 1000, layout = plain / 1000;
          if (base < 1 || base > 7 || layout > 3) throw invalid();
          boolean z = (code & 0x80000000) != 0 || layout == 1 || layout == 3;
          boolean m = (code & 0x40000000) != 0 || layout == 2 || layout == 3;
          int iso = base + (z ? 1000 : 0) + (m ? 2000 : 0);
          int dimensions = 2 + (z ? 1 : 0) + (m ? 1 : 0);
          if ((code & 0x20000000) != 0) input.getInt(); // Caller supplies native SRID.
          changed |= code != iso;
          if (output != null) output.order(order).put(endian).putInt(iso);
          outputSize += 5;
          switch (base) {
            case 1:
              coordinates(dimensions * 8L);
              break;
            case 2:
              coordinates(count() * (dimensions * 8L));
              break;
            case 3:
              int rings = count();
              if (rings > input.remaining() / 4) throw invalid();
              for (int i = 0; i < rings; i++) coordinates(count() * (dimensions * 8L));
              break;
            default:
              pending += count();
          }
          if (pending > input.remaining() / 5) throw invalid();
        }
        coordinates(input.remaining()); // Leave trailing-byte acceptance to Spark.
        return outputSize;
      } catch (BufferUnderflowException e) {
        throw invalid();
      }
    }
  }
}
