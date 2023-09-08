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
package org.apache.sedona.viz.core.Serde;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.log4j.Logger;
import org.apache.sedona.viz.core.ImageSerializableWrapper;

import javax.imageio.ImageIO;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class ImageWrapperSerializer
        extends Serializer<ImageSerializableWrapper>
{
    final static Logger log = Logger.getLogger(ImageWrapperSerializer.class);

    @Override
    public void write(Kryo kryo, Output output, ImageSerializableWrapper object)
    {
        try {
            log.debug("Serializing ImageSerializableWrapper...");
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            ImageIO.write(object.getImage(), "png", byteArrayOutputStream);
            output.writeInt(byteArrayOutputStream.size());
            output.write(byteArrayOutputStream.toByteArray());
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    public byte[] writeImage(ImageSerializableWrapper object)
    {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        try {
            ImageIO.write(object.getImage(), "png", byteArrayOutputStream);
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        int arraySize = byteArrayOutputStream.size();
        Output output = new Output(arraySize + 4);
        output.writeInt(arraySize);
        output.write(byteArrayOutputStream.toByteArray());
        return output.toBytes();
    }

    @Override
    public ImageSerializableWrapper read(Kryo kryo, Input input, Class<ImageSerializableWrapper> type)
    {
        try {
            log.debug("De-serializing ImageSerializableWrapper...");
            int length = input.readInt();
            byte[] inputData = new byte[length];
            input.read(inputData);
            return new ImageSerializableWrapper(ImageIO.read(new ByteArrayInputStream(inputData)));
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public ImageSerializableWrapper readImage(byte[] inputArray)
    {
        Kryo kryo = new Kryo();
        Input input = new Input(inputArray);
        return read(kryo, input, ImageSerializableWrapper.class);
    }
}
