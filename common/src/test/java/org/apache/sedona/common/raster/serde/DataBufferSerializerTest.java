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
import com.esotericsoftware.kryo.io.UnsafeOutput;
import java.awt.Point;
import java.awt.image.BandedSampleModel;
import java.awt.image.DataBuffer;
import java.awt.image.DataBufferByte;
import java.awt.image.DataBufferDouble;
import java.awt.image.DataBufferFloat;
import java.awt.image.DataBufferInt;
import java.awt.image.DataBufferShort;
import java.awt.image.DataBufferUShort;
import java.awt.image.Raster;
import java.awt.image.WritableRaster;
import java.lang.reflect.Array;
import java.util.Arrays;
import org.junit.Assert;
import org.junit.Test;

public class DataBufferSerializerTest extends KryoSerializerTestBase {
  private static final DataBufferSerializer serializer = new DataBufferSerializer();

  @Test
  public void rejectOversizedBankPayloadsBeforeCopyingPixels() {
    // The writer emits every bank in full, even when banks share the same backing array.
    // Each buffer below needs 2 GiB on the wire but only 1-8 MiB of pixel storage in this test.
    DataBuffer[] buffers = {
      new DataBufferByte((byte[][]) repeatBank(new byte[1 << 20], 2048), 1),
      new DataBufferShort((short[][]) repeatBank(new short[1 << 20], 1024), 1),
      new DataBufferUShort((short[][]) repeatBank(new short[1 << 20], 1024), 1),
      new DataBufferInt((int[][]) repeatBank(new int[1 << 20], 512), 1),
      new DataBufferFloat((float[][]) repeatBank(new float[1 << 20], 512), 1),
      new DataBufferDouble((double[][]) repeatBank(new double[1 << 20], 256), 1)
    };
    for (DataBuffer buffer : buffers) {
      // Bound output allocation so the unfixed writer fails safely, without allocating GiB.
      try (Output out = new UnsafeOutput(65536, 65536)) {
        IllegalArgumentException error =
            Assert.assertThrows(
                IllegalArgumentException.class, () -> serializer.write(kryo, out, buffer));
        Assert.assertTrue(error.getMessage().contains("too large to serialize"));
        Assert.assertTrue(error.getMessage().contains("RS_TileExplode"));
        // Only the data type, offsets array, and logical size should have been written.
        Assert.assertEquals(
            "No pixel bank should have been copied",
            (3 + buffer.getNumBanks()) * Integer.BYTES,
            out.position());
      }
    }
  }

  @Test
  public void includeExistingOutputAndBankHeadersInSizeLimit() {
    float[][] banks = (float[][]) repeatBank(new float[1 << 20], 512);
    banks[511] = new float[(1 << 20) - 2048];
    // Pixel payload is 2 GiB - 8192 bytes. With 4096 bytes already written and
    // 4112 bytes of DataBuffer headers, the complete value needs 2147483664 bytes.
    DataBufferFloat buffer = new DataBufferFloat(banks, 1);
    try (Output out = new UnsafeOutput(65536, 65536)) {
      out.writeBytes(new byte[4096]);
      IllegalArgumentException error =
          Assert.assertThrows(
              IllegalArgumentException.class, () -> serializer.write(kryo, out, buffer));
      Assert.assertTrue(error.getMessage().contains("2147483664"));
      Assert.assertTrue(error.getMessage().contains("RS_TileExplode"));
    }
  }

  @Test
  public void serializeChildRasterWithoutUnusedParentBanks() {
    float[] bank = new float[1 << 20];
    bank[0] = 42;
    DataBufferFloat buffer = new DataBufferFloat((float[][]) repeatBank(bank, 512), bank.length);
    WritableRaster parent =
        Raster.createWritableRaster(
            new BandedSampleModel(DataBuffer.TYPE_FLOAT, 1024, 1024, 512), buffer, new Point());
    WritableRaster child = parent.createWritableChild(0, 0, 2, 2, 0, 0, new int[] {0});
    AWTRasterSerializer rasterSerializer = new AWTRasterSerializer();
    try (Output out = createOutput()) {
      rasterSerializer.write(kryo, out, child);
      Assert.assertTrue("Only the small child should be serialized", out.position() < 1024);
      try (Input in = createInput(out)) {
        Raster restored = rasterSerializer.read(kryo, in, Raster.class);
        Assert.assertEquals(2, restored.getWidth());
        Assert.assertEquals(2, restored.getHeight());
        Assert.assertEquals(1, restored.getNumBands());
        Assert.assertEquals(42, restored.getSampleFloat(0, 0, 0), 0);
      }
    }
  }

  private static Object[] repeatBank(Object bank, int count) {
    Object[] banks = (Object[]) Array.newInstance(bank.getClass(), count);
    Arrays.fill(banks, bank);
    return banks;
  }

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
