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
import org.geotools.referencing.operation.transform.AffineTransform2D;

/**
 * AffineTransform2D cannot be correctly deserialized by the default serializer of Kryo, so we need
 * to provide a custom serializer.
 */
public class AffineTransform2DSerializer extends Serializer<AffineTransform2D> {
  @Override
  public void write(Kryo kryo, Output output, AffineTransform2D affineTransform2D) {
    output.writeDouble(affineTransform2D.getScaleX());
    output.writeDouble(affineTransform2D.getShearY());
    output.writeDouble(affineTransform2D.getShearX());
    output.writeDouble(affineTransform2D.getScaleY());
    output.writeDouble(affineTransform2D.getTranslateX());
    output.writeDouble(affineTransform2D.getTranslateY());
  }

  @Override
  public AffineTransform2D read(Kryo kryo, Input input, Class<AffineTransform2D> aClass) {
    double scaleX = input.readDouble();
    double skewY = input.readDouble();
    double skewX = input.readDouble();
    double scaleY = input.readDouble();
    double upperLeftX = input.readDouble();
    double upperLeftY = input.readDouble();
    return new AffineTransform2D(scaleX, skewY, skewX, scaleY, upperLeftX, upperLeftY);
  }
}
