/*
 * FILE: ShapeReaderFactory
 * Copyright (c) 2015 - 2018 GeoSpark Development Team
 *
 * MIT License
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */

package org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils.shp;

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
