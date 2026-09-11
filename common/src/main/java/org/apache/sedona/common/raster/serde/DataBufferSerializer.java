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

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.sun.media.jai.util.DataBufferUtils;
import java.awt.image.DataBuffer;
import java.awt.image.DataBufferByte;
import java.awt.image.DataBufferInt;
import java.awt.image.DataBufferShort;
import java.awt.image.DataBufferUShort;
import java.lang.reflect.Array;

public class DataBufferSerializer extends Serializer<DataBuffer> {
  @Override
  public void write(Kryo kryo, Output output, DataBuffer dataBuffer) {
    int dataType = dataBuffer.getDataType();
    output.writeInt(dataType);
    KryoUtil.writeIntArray(output, dataBuffer.getOffsets());
    output.writeInt(dataBuffer.getSize());
    switch (dataType) {
      case DataBuffer.TYPE_BYTE:
        byte[][] byteDataArray = ((DataBufferByte) dataBuffer).getBankData();
        checkBufferSize(output, byteDataArray, Byte.BYTES);
        KryoUtil.writeByteArrays(output, byteDataArray);
        break;
      case DataBuffer.TYPE_USHORT:
        short[][] uShortDataArray = ((DataBufferUShort) dataBuffer).getBankData();
        checkBufferSize(output, uShortDataArray, Short.BYTES);
        KryoUtil.writeShortArrays(output, uShortDataArray);
        break;
      case DataBuffer.TYPE_SHORT:
        short[][] shortDataArray = ((DataBufferShort) dataBuffer).getBankData();
        checkBufferSize(output, shortDataArray, Short.BYTES);
        KryoUtil.writeShortArrays(output, shortDataArray);
        break;
      case DataBuffer.TYPE_INT:
        int[][] intDataArray = ((DataBufferInt) dataBuffer).getBankData();
        checkBufferSize(output, intDataArray, Integer.BYTES);
        KryoUtil.writeIntArrays(output, intDataArray);
        break;
      case DataBuffer.TYPE_FLOAT:
        float[][] floatDataArray = DataBufferUtils.getBankDataFloat(dataBuffer);
        checkBufferSize(output, floatDataArray, Float.BYTES);
        KryoUtil.writeFloatArrays(output, floatDataArray);
        break;
      case DataBuffer.TYPE_DOUBLE:
        double[][] doubleDataArray = DataBufferUtils.getBankDataDouble(dataBuffer);
        checkBufferSize(output, doubleDataArray, Double.BYTES);
        KryoUtil.writeDoubleArrays(output, doubleDataArray);
        break;
      default:
        throw new RuntimeException("Unknown data type: " + dataType);
    }
  }

  private static void checkBufferSize(Output output, Object[] banks, int bytesPerElement) {
    // Serde.serialize uses one in-memory output. Stream-backed outputs can flush their buffers.
    if (output.getOutputStream() != null) {
      return;
    }
    // Count actual backing arrays: packed pixels, padding, and shared banks make logical
    // width/height/band counts insufficient. Existing raster metadata is already in the output.
    long requiredBytes = (long) output.position() + Integer.BYTES;
    for (Object bank : banks) {
      requiredBytes += Integer.BYTES + (long) Array.getLength(bank) * bytesPerElement;
    }
    // Kryo 4's maximum capacity for an in-memory Output constructed with maxBufferSize = -1.
    long maxBytes = Integer.MAX_VALUE - 8L;
    if (requiredBytes > maxBytes) {
      throw new IllegalArgumentException(
          "Raster is too large to serialize as a single value: requires "
              + requiredBytes
              + " bytes, exceeding the "
              + maxBytes
              + " byte limit. Use the raster reader with retile=true and tileWidth/tileHeight, "
              + "or RS_TileExplode, to serialize smaller tiles.");
    }
  }

  @Override
  public DataBuffer read(Kryo kryo, Input input, Class<DataBuffer> type) {
    int dataType = input.readInt();
    int[] offsets = KryoUtil.readIntArray(input);
    int size = input.readInt();
    DataBuffer dataBuffer;
    switch (dataType) {
      case DataBuffer.TYPE_BYTE:
        byte[][] byteDataArray = KryoUtil.readByteArrays(input);
        dataBuffer = new DataBufferByte(byteDataArray, size, offsets);
        break;
      case DataBuffer.TYPE_USHORT:
        short[][] uShortDataArray = KryoUtil.readShortArrays(input);
        dataBuffer = new DataBufferUShort(uShortDataArray, size, offsets);
        break;
      case DataBuffer.TYPE_SHORT:
        short[][] shortDataArray = KryoUtil.readShortArrays(input);
        dataBuffer = new DataBufferShort(shortDataArray, size, offsets);
        break;
      case DataBuffer.TYPE_INT:
        int[][] intDataArray = KryoUtil.readIntArrays(input);
        dataBuffer = new DataBufferInt(intDataArray, size, offsets);
        break;
      case DataBuffer.TYPE_FLOAT:
        float[][] floatDataArray = KryoUtil.readFloatArrays(input);
        dataBuffer = DataBufferUtils.createDataBufferFloat(floatDataArray, size, offsets);
        break;
      case DataBuffer.TYPE_DOUBLE:
        double[][] doubleDataArray = KryoUtil.readDoubleArrays(input);
        dataBuffer = DataBufferUtils.createDataBufferDouble(doubleDataArray, size, offsets);
        break;
      default:
        throw new RuntimeException("Unknown data type: " + dataType);
    }
    return dataBuffer;
  }
}
