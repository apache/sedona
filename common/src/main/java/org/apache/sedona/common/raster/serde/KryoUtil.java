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
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * Utility methods for serializing objects with Kryo. The serialization formats are well-defined and
 * independent of the Kryo version. This allows us to exchange serialized data with other tech
 * stack, such as Python.
 */
public class KryoUtil {
  private KryoUtil() {}

  /**
   * Write the length of the next serialized object, followed by the serialized object
   *
   * @param kryo the kryo instance
   * @param output the output stream
   * @param object the object to serialize
   */
  public static void writeObjectWithLength(Kryo kryo, Output output, Object object) {
    int lengthOffset = output.position();
    output.writeInt(0); // placeholder, will be overwritten later

    // Write the object
    int start = output.position();
    kryo.writeObject(output, object);
    int end = output.position();

    // Rewrite the length
    int length = end - start;
    output.setPosition(lengthOffset);
    output.writeInt(length);
    output.setPosition(end);
  }

  /**
   * Write string as UTF-8 byte sequence
   *
   * @param output the output stream
   * @param value the string to write
   */
  public static void writeUTF8String(Output output, String value) {
    byte[] utf8 = value.getBytes(StandardCharsets.UTF_8);
    output.writeInt(utf8.length);
    output.writeBytes(utf8);
  }

  /**
   * Read UTF-8 byte sequence as string
   *
   * @param input the input stream
   * @return the string
   */
  public static String readUTF8String(Input input) {
    int length = input.readInt();
    byte[] utf8 = new byte[length];
    input.readBytes(utf8);
    return new String(utf8, StandardCharsets.UTF_8);
  }

  /**
   * Write an array of integers
   *
   * @param output the output stream
   * @param array the array to write
   */
  public static void writeIntArray(Output output, int[] array) {
    output.writeInt(array.length);
    output.writeInts(array);
  }

  /**
   * Read an array of integers
   *
   * @param input the input stream
   * @return the array
   */
  public static int[] readIntArray(Input input) {
    int length = input.readInt();
    return input.readInts(length);
  }

  /**
   * Write a 2-d array of ints
   *
   * @param output the output stream
   * @param arrays the array to write
   */
  public static void writeIntArrays(Output output, int[][] arrays) {
    output.writeInt(arrays.length);
    for (int[] array : arrays) {
      writeIntArray(output, array);
    }
  }

  /**
   * Read a 2-d array of ints
   *
   * @param input the input stream
   * @return the array
   */
  public static int[][] readIntArrays(Input input) {
    int length = input.readInt();
    int[][] arrays = new int[length][];
    for (int i = 0; i < length; i++) {
      arrays[i] = readIntArray(input);
    }
    return arrays;
  }

  /**
   * Write a 2-d array of bytes
   *
   * @param output the output stream
   * @param arrays the array to write
   */
  public static void writeByteArrays(Output output, byte[][] arrays) {
    output.writeInt(arrays.length);
    for (byte[] array : arrays) {
      output.writeInt(array.length);
      output.writeBytes(array);
    }
  }

  /**
   * Read a 2-d array of bytes
   *
   * @param input the input stream
   * @return the array
   */
  public static byte[][] readByteArrays(Input input) {
    int length = input.readInt();
    byte[][] arrays = new byte[length][];
    for (int i = 0; i < length; i++) {
      int arrayLength = input.readInt();
      arrays[i] = input.readBytes(arrayLength);
    }
    return arrays;
  }

  /**
   * Write a 2-d array of doubles
   *
   * @param output the output stream
   * @param arrays the array to write
   */
  public static void writeDoubleArrays(Output output, double[][] arrays) {
    output.writeInt(arrays.length);
    for (double[] array : arrays) {
      output.writeInt(array.length);
      output.writeDoubles(array);
    }
  }

  /**
   * Read a 2-d array of doubles
   *
   * @param input the input stream
   * @return the array
   */
  public static double[][] readDoubleArrays(Input input) {
    int length = input.readInt();
    double[][] arrays = new double[length][];
    for (int i = 0; i < length; i++) {
      int arrayLength = input.readInt();
      arrays[i] = input.readDoubles(arrayLength);
    }
    return arrays;
  }

  /**
   * Write a 2-d array of longs
   *
   * @param output the output stream
   * @param arrays the array to write
   */
  public static void writeLongArrays(Output output, long[][] arrays) {
    output.writeInt(arrays.length);
    for (long[] array : arrays) {
      output.writeInt(array.length);
      output.writeLongs(array);
    }
  }

  /**
   * Read a 2-d array of longs
   *
   * @param input the input stream
   * @return the array
   */
  public static long[][] readLongArrays(Input input) {
    int length = input.readInt();
    long[][] arrays = new long[length][];
    for (int i = 0; i < length; i++) {
      int arrayLength = input.readInt();
      arrays[i] = input.readLongs(arrayLength);
    }
    return arrays;
  }

  /**
   * Write a 2-d array of floats
   *
   * @param output the output stream
   * @param arrays the array to write
   */
  public static void writeFloatArrays(Output output, float[][] arrays) {
    output.writeInt(arrays.length);
    for (float[] array : arrays) {
      output.writeInt(array.length);
      output.writeFloats(array);
    }
  }

  /**
   * Read a 2-d array of floats
   *
   * @param input the input stream
   * @return the array
   */
  public static float[][] readFloatArrays(Input input) {
    int length = input.readInt();
    float[][] arrays = new float[length][];
    for (int i = 0; i < length; i++) {
      int arrayLength = input.readInt();
      arrays[i] = input.readFloats(arrayLength);
    }
    return arrays;
  }

  /**
   * Write a 2-d array of shorts
   *
   * @param output the output stream
   * @param arrays the array to write
   */
  public static void writeShortArrays(Output output, short[][] arrays) {
    output.writeInt(arrays.length);
    for (short[] array : arrays) {
      output.writeInt(array.length);
      output.writeShorts(array);
    }
  }

  /**
   * Read a 2-d array of shorts
   *
   * @param input the input stream
   * @return the array
   */
  public static short[][] readShortArrays(Input input) {
    int length = input.readInt();
    short[][] arrays = new short[length][];
    for (int i = 0; i < length; i++) {
      int arrayLength = input.readInt();
      arrays[i] = input.readShorts(arrayLength);
    }
    return arrays;
  }

  /**
   * Write a {@code Map<String, String>} object to the output stream
   *
   * @param output the output stream
   * @param map the map to write
   */
  public static void writeUTF8StringMap(Output output, Map<String, String> map) {
    if (map == null) {
      output.writeInt(-1);
      return;
    }
    output.writeInt(map.size());
    for (Map.Entry<String, String> entry : map.entrySet()) {
      writeUTF8String(output, entry.getKey());
      writeUTF8String(output, entry.getValue());
    }
  }

  /**
   * Read a {@code Map<String, String>} object from the input stream
   *
   * @param input the input stream
   * @return the map
   */
  public static Map<String, String> readUTF8StringMap(Input input) {
    int size = input.readInt();
    if (size == -1) {
      return null;
    }
    Map<String, String> params = new HashMap<>(size);
    for (int i = 0; i < size; i++) {
      String key = readUTF8String(input);
      String value = readUTF8String(input);
      params.put(key, value);
    }
    return params;
  }
}
