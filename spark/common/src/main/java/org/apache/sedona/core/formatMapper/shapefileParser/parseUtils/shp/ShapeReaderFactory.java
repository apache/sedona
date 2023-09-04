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

package org.apache.sedona.core.formatMapper.shapefileParser.parseUtils.shp;

import com.esotericsoftware.kryo.io.Input;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class ShapeReaderFactory
{

    public static ShapeReader fromByteBuffer(final ByteBuffer buffer)
    {
        final ByteBuffer leBuffer = buffer.slice().order(ByteOrder.LITTLE_ENDIAN);
        return new ShapeReader()
        {
            @Override
            public int readInt()
            {
                return leBuffer.getInt();
            }

            @Override
            public double readDouble()
            {
                return leBuffer.getDouble();
            }

            @Override
            public byte readByte()
            {
                return leBuffer.get();
            }

            @Override
            public void skip(int numBytes)
            {
                leBuffer.position(leBuffer.position() + numBytes);
            }
        };
    }

    public static ShapeReader fromInput(final Input input)
    {
        return new ShapeReader()
        {
            @Override
            public int readInt()
            {
                return toByteBuffer(input, 4).getInt();
            }

            @Override
            public double readDouble()
            {
                return toByteBuffer(input, 8).getDouble();
            }

            @Override
            public byte readByte()
            {
                return input.readByte();
            }

            @Override
            public void skip(int numBytes)
            {
                input.skip(numBytes);
            }
        };
    }

    private static ByteBuffer toByteBuffer(Input input, int numBytes)
    {
        byte[] bytes = new byte[numBytes];
        input.read(bytes);
        return ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
    }
}
