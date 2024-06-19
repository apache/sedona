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

import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import java.awt.image.DataBuffer;
import java.awt.image.DataBufferByte;
import java.awt.image.DataBufferDouble;
import java.awt.image.DataBufferFloat;
import java.awt.image.DataBufferInt;
import java.awt.image.DataBufferShort;
import java.awt.image.DataBufferUShort;
import org.junit.Assert;
import org.junit.Test;

public class DataBufferSerializerTest extends KryoSerializerTestBase {
  private static final DataBufferSerializer serializer = new DataBufferSerializer();

  private static void assertEquals(DataBuffer expected, DataBuffer actual) {
    Assert.assertEquals(expected.getDataType(), actual.getDataType());
    Assert.assertEquals(expected.getNumBanks(), actual.getNumBanks());
    Assert.assertEquals(expected.getSize(), actual.getSize());
    Assert.assertArrayEquals(expected.getOffsets(), actual.getOffsets());
    for (int bank = 0; bank < expected.getNumBanks(); bank++) {
      for (int k = 0; k < expected.getSize(); k++) {
        Assert.assertEquals(expected.getElemDouble(bank, k), actual.getElemDouble(bank, k), 1e-6);
      }
    }
  }

  @Test
  public void serializeByteBuffer() {
    byte[][] dataArray = {
      {1, 2, 3, 4, 5},
      {6, 7, 8, 9, 0}
    };
    int size = 5;
    int[] offsets = {0, 0};
    DataBufferByte dataBufferByte = new DataBufferByte(dataArray, size, offsets);
    try (Output out = createOutput()) {
      serializer.write(kryo, out, dataBufferByte);
      try (Input in = createInput(out)) {
        DataBuffer dataBufferByte1 = serializer.read(kryo, in, DataBuffer.class);
        assertEquals(dataBufferByte, dataBufferByte1);
      }
    }
  }

  @Test
  public void serializeShortBuffer() {
    short[][] dataArray = {
      {1, 2, 3, 4, 5},
      {6, 7, 8, 9, 0}
    };
    int size = 5;
    int[] offsets = {0, 0};
    DataBuffer dataBufferShort = new DataBufferShort(dataArray, size, offsets);
    try (Output out = createOutput()) {
      serializer.write(kryo, out, dataBufferShort);
      try (Input in = createInput(out)) {
        DataBuffer dataBufferShort1 = serializer.read(kryo, in, DataBuffer.class);
        Assert.assertTrue(dataBufferShort1 instanceof DataBufferShort);
        assertEquals(dataBufferShort, dataBufferShort1);
      }
    }
  }

  @Test
  public void serializeUShortBuffer() {
    short[][] dataArray = {
      {1, 2, 3, 4, 5},
      {6, 7, 8, 9, 0}
    };
    int size = 5;
    int[] offsets = {0, 0};
    DataBuffer dataBufferShort = new DataBufferUShort(dataArray, size, offsets);
    try (Output out = createOutput()) {
      serializer.write(kryo, out, dataBufferShort);
      try (Input in = createInput(out)) {
        DataBuffer dataBufferShort1 = serializer.read(kryo, in, DataBuffer.class);
        Assert.assertTrue(dataBufferShort1 instanceof DataBufferUShort);
        assertEquals(dataBufferShort, dataBufferShort1);
      }
    }
  }

  @Test
  public void serializeIntBuffer() {
    int[][] dataArray = {
      {1, 2, 3, 4, 5},
      {6, 7, 8, 9, 0}
    };
    int size = 5;
    int[] offsets = {0, 0};
    DataBuffer dataBufferInt = new DataBufferInt(dataArray, size, offsets);
    try (Output out = createOutput()) {
      serializer.write(kryo, out, dataBufferInt);
      try (Input in = createInput(out)) {
        DataBuffer dataBufferInt1 = serializer.read(kryo, in, DataBuffer.class);
        assertEquals(dataBufferInt, dataBufferInt1);
      }
    }
  }

  @Test
  public void serializeFloatBuffer() {
    float[][] dataArray = {
      {1, 2, 3, 4, 5},
      {6, 7, 8, 9, 0}
    };
    int size = 5;
    int[] offsets = {0, 0};
    DataBuffer dataBufferFloat = new DataBufferFloat(dataArray, size, offsets);
    try (Output out = createOutput()) {
      serializer.write(kryo, out, dataBufferFloat);
      try (Input in = createInput(out)) {
        DataBuffer dataBufferFloat1 = serializer.read(kryo, in, DataBuffer.class);
        assertEquals(dataBufferFloat, dataBufferFloat1);
      }
    }
  }

  @Test
  public void serializeDoubleBuffer() {
    double[][] dataArray = {
      {1, 2, 3, 4, 5},
      {6, 7, 8, 9, 0}
    };
    int size = 5;
    int[] offsets = {0, 0};
    DataBuffer dataBufferDouble = new DataBufferDouble(dataArray, size, offsets);
    try (Output out = createOutput()) {
      serializer.write(kryo, out, dataBufferDouble);
      try (Input in = createInput(out)) {
        DataBuffer dataBufferDouble1 = serializer.read(kryo, in, DataBuffer.class);
        assertEquals(dataBufferDouble, dataBufferDouble1);
      }
    }
  }
}
