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
import java.awt.image.BandedSampleModel;
import java.awt.image.ComponentSampleModel;
import java.awt.image.MultiPixelPackedSampleModel;
import java.awt.image.PixelInterleavedSampleModel;
import java.awt.image.SampleModel;
import java.awt.image.SinglePixelPackedSampleModel;
import javax.media.jai.ComponentSampleModelJAI;
import javax.media.jai.RasterFactory;

/**
 * Serializer for SampleModelState using Kryo. This is translated from the original JAI
 * implementation of writeObject and readObject.
 */
public class SampleModelSerializer extends Serializer<SampleModel> {

  // These constants are taken from SampleModelState
  private static final int TYPE_BANDED = 1;
  private static final int TYPE_PIXEL_INTERLEAVED = 2;
  private static final int TYPE_SINGLE_PIXEL_PACKED = 3;
  private static final int TYPE_MULTI_PIXEL_PACKED = 4;
  private static final int TYPE_COMPONENT_JAI = 5;
  private static final int TYPE_COMPONENT = 6;

  private static int sampleModelTypeOf(SampleModel sampleModel) {
    if (sampleModel instanceof ComponentSampleModel) {
      if (sampleModel instanceof PixelInterleavedSampleModel) {
        return TYPE_PIXEL_INTERLEAVED;
      } else if (sampleModel instanceof BandedSampleModel) {
        return TYPE_BANDED;
      } else if (sampleModel instanceof ComponentSampleModelJAI) {
        return TYPE_COMPONENT_JAI;
      } else {
        return TYPE_COMPONENT;
      }
    } else if (sampleModel instanceof SinglePixelPackedSampleModel) {
      return TYPE_SINGLE_PIXEL_PACKED;
    } else if (sampleModel instanceof MultiPixelPackedSampleModel) {
      return TYPE_MULTI_PIXEL_PACKED;
    } else {
      throw new UnsupportedOperationException(
          "Unsupported SampleModel type: " + sampleModel.getClass().getName());
    }
  }

  @Override
  public void write(Kryo kryo, Output output, SampleModel sampleModel) {
    int sampleModelType = sampleModelTypeOf(sampleModel);
    output.writeInt(sampleModelType);
    output.writeInt(sampleModel.getTransferType());
    output.writeInt(sampleModel.getWidth());
    output.writeInt(sampleModel.getHeight());

    switch (sampleModelType) {
      case TYPE_BANDED:
        {
          BandedSampleModel sm = (BandedSampleModel) sampleModel;
          KryoUtil.writeIntArray(output, sm.getBankIndices());
          KryoUtil.writeIntArray(output, sm.getBandOffsets());
          break;
        }

      case TYPE_PIXEL_INTERLEAVED:
        {
          PixelInterleavedSampleModel sm = (PixelInterleavedSampleModel) sampleModel;
          output.writeInt(sm.getPixelStride());
          output.writeInt(sm.getScanlineStride());
          KryoUtil.writeIntArray(output, sm.getBandOffsets());
          break;
        }

      case TYPE_COMPONENT:
      case TYPE_COMPONENT_JAI:
        {
          ComponentSampleModel sm = (ComponentSampleModel) sampleModel;
          output.writeInt(sm.getPixelStride());
          output.writeInt(sm.getScanlineStride());
          KryoUtil.writeIntArray(output, sm.getBankIndices());
          KryoUtil.writeIntArray(output, sm.getBandOffsets());
          break;
        }

      case TYPE_SINGLE_PIXEL_PACKED:
        {
          SinglePixelPackedSampleModel sm = (SinglePixelPackedSampleModel) sampleModel;
          output.writeInt(sm.getScanlineStride());
          KryoUtil.writeIntArray(output, sm.getBitMasks());
          break;
        }

      case TYPE_MULTI_PIXEL_PACKED:
        {
          MultiPixelPackedSampleModel sm = (MultiPixelPackedSampleModel) sampleModel;
          output.writeInt(sm.getPixelBitStride());
          output.writeInt(sm.getScanlineStride());
          output.writeInt(sm.getDataBitOffset());
          break;
        }

      default:
        throw new UnsupportedOperationException(
            "Unknown SampleModel type: " + sampleModel.getClass().getName());
    }
  }

  @Override
  public SampleModel read(Kryo kryo, Input input, Class<SampleModel> type) {
    int sampleModelType = input.readInt();
    int transferType = input.readInt();
    int width = input.readInt();
    int height = input.readInt();

    switch (sampleModelType) {
      case TYPE_BANDED:
        {
          int[] bankIndices = KryoUtil.readIntArray(input);
          int[] bandOffsets = KryoUtil.readIntArray(input);
          return RasterFactory.createBandedSampleModel(
              transferType, width, height, bankIndices.length, bankIndices, bandOffsets);
        }

      case TYPE_PIXEL_INTERLEAVED:
        {
          int pixelStride = input.readInt();
          int scanLineStride = input.readInt();
          int[] bandOffsets = KryoUtil.readIntArray(input);
          return RasterFactory.createPixelInterleavedSampleModel(
              transferType, width, height, pixelStride, scanLineStride, bandOffsets);
        }

      case TYPE_COMPONENT_JAI:
      case TYPE_COMPONENT:
        {
          int pixelStride = input.readInt();
          int scanLineStride = input.readInt();
          int[] bankIndices = KryoUtil.readIntArray(input);
          int[] bandOffsets = KryoUtil.readIntArray(input);
          if (sampleModelType == TYPE_COMPONENT_JAI) {
            return new ComponentSampleModelJAI(
                transferType, width, height, pixelStride, scanLineStride, bankIndices, bandOffsets);
          } else {
            return new ComponentSampleModel(
                transferType, width, height, pixelStride, scanLineStride, bankIndices, bandOffsets);
          }
        }

      case TYPE_SINGLE_PIXEL_PACKED:
        {
          int scanLineStride = input.readInt();
          int[] bitMasks = KryoUtil.readIntArray(input);
          return new SinglePixelPackedSampleModel(
              transferType, width, height, scanLineStride, bitMasks);
        }

      case TYPE_MULTI_PIXEL_PACKED:
        {
          int pixelStride = input.readInt();
          int scanLineStride = input.readInt();
          int dataBitOffset = input.readInt();
          return new MultiPixelPackedSampleModel(
              transferType, width, height, pixelStride, scanLineStride, dataBitOffset);
        }

      default:
        throw new UnsupportedOperationException("Unsupported SampleModel type: " + sampleModelType);
    }
  }
}
