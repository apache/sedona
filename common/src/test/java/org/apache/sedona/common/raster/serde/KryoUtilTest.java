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
package org.apache.sedona.common.raster.serde;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.junit.Assert;
import org.junit.Test;

public class KryoUtilTest extends KryoSerializerTestBase {

  private static class TestClass {
    private int a;
    private String b;
    private double[] c;

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      TestClass testClass = (TestClass) o;
      return a == testClass.a && Objects.equals(b, testClass.b) && Arrays.equals(c, testClass.c);
    }

    @Override
    public int hashCode() {
      return Objects.hash(a, b, Arrays.hashCode(c));
    }
  }

  @Test
  public void writeObjectWithLength() {
    TestClass obj = new TestClass();
    obj.a = 1;
    obj.b = "test";
    obj.c = new double[] {1.0, 2.0, 3.0};
    try (Output out = createOutput()) {
      KryoUtil.writeObjectWithLength(kryo, out, obj);
      try (Input in = createInput(out)) {
        in.readInt(); // skip serialized object length
        TestClass obj2 = kryo.readObject(in, TestClass.class);
        assertEquals(obj, obj2);
      }
    }
  }

  @Test
  public void serializeUTF8String() {
    String str =
        "Hello - English\n"
            + "Hola - Spanish\n"
            + "Bonjour - French\n"
            + "Hallo - German\n"
            + "Ciao - Italian\n"
            + "你好 - Chinese\n"
            + "こんにちは - Japanese\n"
            + "안녕하세요 - Korean\n"
            + "Здравствуйте - Russian\n"
            + "नमस्ते - Hindi\n"
            + "مرحبا - Arabic\n"
            + "שלום - Hebrew\n"
            + "สวัสดี - Thai\n"
            + "Merhaba - Turkish\n"
            + "Γεια σας - Greek";
    try (Output out = createOutput()) {
      KryoUtil.writeUTF8String(out, str);
      try (Input in = createInput(out)) {
        String str2 = KryoUtil.readUTF8String(in);
        assertEquals(str, str2);
      }
    }
  }

  @Test
  public void serializeIntArray() {
    int[] arr = new int[] {1, 2, 3, 4, 5};
    try (Output out = createOutput()) {
      KryoUtil.writeIntArray(out, arr);
      try (Input in = createInput(out)) {
        int[] arr2 = KryoUtil.readIntArray(in);
        assertArrayEquals(arr, arr2);
      }
    }
  }

  @Test
  public void serializeIntArrays() {
    int[][] arrs =
        new int[][] {
          new int[] {1, 2, 3, 4, 5},
          new int[] {6, 7, 8, 9, 10}
        };
    try (Output out = createOutput()) {
      KryoUtil.writeIntArrays(out, arrs);
      try (Input in = createInput(out)) {
        int[][] arrs2 = KryoUtil.readIntArrays(in);
        assertArrayEquals(arrs, arrs2);
      }
    }
  }

  @Test
  public void serializeByteArrays() {
    byte[][] arrs =
        new byte[][] {
          new byte[] {1, 2, 3, 4, 5},
          new byte[] {6, 7, 8, 9, 10}
        };
    try (Output out = createOutput()) {
      KryoUtil.writeByteArrays(out, arrs);
      try (Input in = createInput(out)) {
        byte[][] arrs2 = KryoUtil.readByteArrays(in);
        assertArrayEquals(arrs, arrs2);
      }
    }
  }

  @Test
  public void serializeDoubleArrays() {
    double[][] arrs =
        new double[][] {
          new double[] {1.0, 2.0, 3.0, 4.0, 5.0},
          new double[] {6.0, 7.0, 8.0, 9.0, 10.0}
        };
    try (Output out = createOutput()) {
      KryoUtil.writeDoubleArrays(out, arrs);
      try (Input in = createInput(out)) {
        double[][] arrs2 = KryoUtil.readDoubleArrays(in);
        assertArrayEquals(arrs, arrs2);
      }
    }
  }

  @Test
  public void serializeLongArrays() {
    long[][] arrs =
        new long[][] {
          new long[] {1L, 2L, 3L, 4L, 5L},
          new long[] {6L, 7L, 8L, 9L, 10L}
        };
    try (Output out = createOutput()) {
      KryoUtil.writeLongArrays(out, arrs);
      try (Input in = createInput(out)) {
        long[][] arrs2 = KryoUtil.readLongArrays(in);
        assertArrayEquals(arrs, arrs2);
      }
    }
  }

  @Test
  public void serializeFloatArrays() {
    float[][] arrs =
        new float[][] {
          new float[] {1.0f, 2.0f, 3.0f, 4.0f, 5.0f},
          new float[] {6.0f, 7.0f, 8.0f, 9.0f, 10.0f}
        };
    try (Output out = createOutput()) {
      KryoUtil.writeFloatArrays(out, arrs);
      try (Input in = createInput(out)) {
        float[][] arrs2 = KryoUtil.readFloatArrays(in);
        assertArrayEquals(arrs, arrs2);
      }
    }
  }

  @Test
  public void serializeShortArrays() {
    short[][] arrs =
        new short[][] {
          new short[] {1, 2, 3, 4, 5},
          new short[] {6, 7, 8, 9, 10}
        };
    try (Output out = createOutput()) {
      KryoUtil.writeShortArrays(out, arrs);
      try (Input in = createInput(out)) {
        short[][] arrs2 = KryoUtil.readShortArrays(in);
        assertArrayEquals(arrs, arrs2);
      }
    }
  }

  @Test
  public void serializeNullUTF8StringMap() {
    try (Output out = createOutput()) {
      KryoUtil.writeUTF8StringMap(out, null);
      try (Input in = createInput(out)) {
        Map<String, String> map = KryoUtil.readUTF8StringMap(in);
        Assert.assertNull(map);
      }
    }
  }

  @Test
  public void serializeEmptyUTF8StringMap() {
    try (Output out = createOutput()) {
      KryoUtil.writeUTF8StringMap(out, Collections.emptyMap());
      try (Input in = createInput(out)) {
        Map<String, String> map = KryoUtil.readUTF8StringMap(in);
        Assert.assertNotNull(map);
        Assert.assertTrue(map.isEmpty());
      }
    }
  }

  @Test
  public void serializeUTF8StringMap() {
    Map<String, String> map = new HashMap<>();
    map.put("key1", "value1");
    map.put("key2", "value2");

    try (Output out = createOutput()) {
      KryoUtil.writeUTF8StringMap(out, map);
      try (Input in = createInput(out)) {
        Map<String, String> map2 = KryoUtil.readUTF8StringMap(in);
        Assert.assertNotNull(map2);
        Assert.assertEquals(map, map2);
      }
    }
  }
}
