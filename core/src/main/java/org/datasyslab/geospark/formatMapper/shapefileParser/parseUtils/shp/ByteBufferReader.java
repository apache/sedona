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

// TODO: Auto-generated Javadoc
/**
 * The Class ByteBufferReader.
 */
public class ByteBufferReader extends ShapeReader{

    /** The buffer. */
    private ByteBuffer buffer = null;

    /**
     * construct the reader with byte array.
     *
     * @param bytes the bytes
     * @param endianOrder false = little true = big
     */
    public ByteBufferReader(byte[] bytes, boolean endianOrder){
        buffer = ByteBuffer.wrap(bytes);
        if(endianOrder) buffer.order(ByteOrder.BIG_ENDIAN);
        else buffer.order(ByteOrder.LITTLE_ENDIAN);
    }

    /* (non-Javadoc)
     * @see org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils.shp.ShapeReader#readDouble()
     */
    @Override
    public double readDouble() throws IOException {
        return buffer.getDouble();
    }

    /* (non-Javadoc)
     * @see org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils.shp.ShapeReader#readInt()
     */
    @Override
    public int readInt() throws IOException {
        return buffer.getInt();
    }

    /* (non-Javadoc)
     * @see org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils.shp.ShapeReader#read(byte[])
     */
    @Override
    public void read(byte[] bytes) throws IOException {
        buffer.get(bytes);
    }

    /* (non-Javadoc)
     * @see org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils.shp.ShapeReader#read(byte[], int, int)
     */
    @Override
    public void read(byte[] bytes, int offset, int len) throws IOException {
        buffer.get(bytes, offset, len);
    }

    /* (non-Javadoc)
     * @see org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils.shp.ShapeReader#read(double[])
     */
    @Override
    public void read(double[] doubles) throws IOException {
        DoubleBuffer doubleBuffer = buffer.asDoubleBuffer();
        doubleBuffer.get(doubles);
    }

    /* (non-Javadoc)
     * @see org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils.shp.ShapeReader#skip(int)
     */
    @Override
    public void skip(int n) throws IOException {
        buffer.position(buffer.position() + n);
    }
}
