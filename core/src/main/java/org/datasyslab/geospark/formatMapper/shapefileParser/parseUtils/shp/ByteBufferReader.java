/**
 * FILE: ByteBufferReader.java
 * PATH: org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils.shp.ByteBufferReader.java
 * Copyright (c) 2015-2017 GeoSpark Development Team
 * All rights reserved.
 */
package org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils.shp;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.DoubleBuffer;

public class ByteBufferReader extends ShapeReader{

    private ByteBuffer buffer = null;

    /**
     * construct the reader with byte array
     * @param bytes
     * @param endianOrder false = little true = big
     */
    public ByteBufferReader(byte[] bytes, boolean endianOrder){
        buffer = ByteBuffer.wrap(bytes);
        if(endianOrder) buffer.order(ByteOrder.BIG_ENDIAN);
        else buffer.order(ByteOrder.LITTLE_ENDIAN);
    }

    @Override
    public double readDouble() throws IOException {
        return buffer.getDouble();
    }

    @Override
    public int readInt() throws IOException {
        return buffer.getInt();
    }

    @Override
    public void read(byte[] bytes) throws IOException {
        buffer.get(bytes);
    }

    @Override
    public void read(byte[] bytes, int offset, int len) throws IOException {
        buffer.get(bytes, offset, len);
    }

    @Override
    public void read(double[] doubles) throws IOException {
        DoubleBuffer doubleBuffer = buffer.asDoubleBuffer();
        doubleBuffer.get(doubles);
    }

    @Override
    public void skip(int n) throws IOException {
        buffer.position(buffer.position() + n);
    }
}
