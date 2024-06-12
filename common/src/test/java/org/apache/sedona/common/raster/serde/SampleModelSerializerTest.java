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
import java.awt.image.BandedSampleModel;
import java.awt.image.ComponentSampleModel;
import java.awt.image.DataBuffer;
import java.awt.image.MultiPixelPackedSampleModel;
import java.awt.image.PixelInterleavedSampleModel;
import java.awt.image.SampleModel;
import java.awt.image.SinglePixelPackedSampleModel;
import javax.media.jai.ComponentSampleModelJAI;
import org.junit.Assert;
import org.junit.Test;

public class SampleModelSerializerTest extends KryoSerializerTestBase {
  private static final SampleModelSerializer serializer = new SampleModelSerializer();

  @Test
  public void serializeBandedSampleModel() {
    int[] bankIndices = {2, 0, 1};
    int[] bandOffsets = {4, 8, 12};
    SampleModel sm =
        new BandedSampleModel(DataBuffer.TYPE_INT, 100, 80, 100, bankIndices, bandOffsets);
    try (Output out = createOutput()) {
      serializer.write(kryo, out, sm);
      try (Input in = createInput(out)) {
        SampleModel sm1 = serializer.read(kryo, in, SampleModel.class);
        Assert.assertEquals(sm, sm1);
      }
    }
  }

  @Test
  public void serializePixelInterleavedSampleModel() {
    int[] bandOffsets = {0, 1, 2};
    SampleModel sm =
        new PixelInterleavedSampleModel(DataBuffer.TYPE_INT, 100, 80, 3, 300, bandOffsets);
    try (Output out = createOutput()) {
      serializer.write(kryo, out, sm);
      try (Input in = createInput(out)) {
        SampleModel sm1 = serializer.read(kryo, in, SampleModel.class);
        Assert.assertEquals(sm, sm1);
      }
    }
  }

  @Test
  public void serializeComponentSampleModel() {
    int[] bankIndices = {1, 0};
    int[] bandOffsets = {0, 10000};
    SampleModel sm =
        new ComponentSampleModel(DataBuffer.TYPE_INT, 100, 80, 1, 100, bankIndices, bandOffsets);
    try (Output out = createOutput()) {
      serializer.write(kryo, out, sm);
      try (Input in = createInput(out)) {
        SampleModel sm1 = serializer.read(kryo, in, SampleModel.class);
        Assert.assertEquals(sm, sm1);
      }
    }
  }

  @Test
  public void serializeComponentSampleModelJAI() {
    int[] bankIndices = {1, 0};
    int[] bandOffsets = {0, 10000};
    SampleModel sm =
        new ComponentSampleModelJAI(DataBuffer.TYPE_INT, 100, 80, 1, 100, bankIndices, bandOffsets);
    try (Output out = createOutput()) {
      serializer.write(kryo, out, sm);
      try (Input in = createInput(out)) {
        SampleModel sm1 = serializer.read(kryo, in, SampleModel.class);
        Assert.assertEquals(sm, sm1);
      }
    }
  }

  @Test
  public void serializeSinglePixelPackedSampleModel() {
    int[] bitMasks = {0x000000ff, 0x0000ff00, 0x00ff0000};
    SampleModel sm = new SinglePixelPackedSampleModel(DataBuffer.TYPE_INT, 100, 80, 100, bitMasks);
    try (Output out = createOutput()) {
      serializer.write(kryo, out, sm);
      try (Input in = createInput(out)) {
        SampleModel sm1 = serializer.read(kryo, in, SampleModel.class);
        Assert.assertEquals(sm, sm1);
      }
    }
  }

  @Test
  public void serializedMultiPixelPackedSampleModel() {
    SampleModel sm = new MultiPixelPackedSampleModel(DataBuffer.TYPE_BYTE, 100, 80, 4);
    try (Output out = createOutput()) {
      serializer.write(kryo, out, sm);
      try (Input in = createInput(out)) {
        SampleModel sm1 = serializer.read(kryo, in, SampleModel.class);
        Assert.assertEquals(sm, sm1);
      }
    }
  }
}
