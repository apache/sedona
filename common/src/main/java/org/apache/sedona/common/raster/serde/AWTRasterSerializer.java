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
import java.awt.Point;
import java.awt.image.DataBuffer;
import java.awt.image.Raster;
import java.awt.image.SampleModel;
import java.awt.image.WritableRaster;

public class AWTRasterSerializer extends Serializer<Raster> {
  private static final SampleModelSerializer sampleModelSerializer = new SampleModelSerializer();
  private static final DataBufferSerializer dataBufferSerializer = new DataBufferSerializer();

  @Override
  public void write(Kryo kryo, Output output, Raster raster) {
    Raster r;
    if (raster.getParent() != null) {
      r = raster.createCompatibleWritableRaster(raster.getBounds());
      ((WritableRaster) r).setRect(raster);
    } else {
      r = raster;
    }

    output.writeInt(r.getMinX());
    output.writeInt(r.getMinY());
    sampleModelSerializer.write(kryo, output, r.getSampleModel());
    dataBufferSerializer.write(kryo, output, r.getDataBuffer());
  }

  @Override
  public Raster read(Kryo kryo, Input input, Class<Raster> type) {
    int minX = input.readInt();
    int minY = input.readInt();
    Point location = new Point(minX, minY);
    SampleModel sampleModel = sampleModelSerializer.read(kryo, input, SampleModel.class);
    DataBuffer dataBuffer = dataBufferSerializer.read(kryo, input, DataBuffer.class);
    return Raster.createRaster(sampleModel, dataBuffer, location);
  }
}
