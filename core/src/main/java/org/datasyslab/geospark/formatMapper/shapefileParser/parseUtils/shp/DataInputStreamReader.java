/**
 * FILE: DataInputStreamReader.java
 * PATH: org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils.shp.DataInputStreamReader.java
 * Copyright (c) 2015-2017 GeoSpark Development Team
 * All rights reserved.
 */
package org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils.shp;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.DoubleBuffer;

// TODO: Auto-generated Javadoc
/**
 * The Class DataInputStreamReader.
 */
public class DataInputStreamReader extends ShapeReader {

    /** The input stream. */
    private DataInputStream inputStream = null;

    /**
     * Instantiates a new data input stream reader.
     *
     * @param dis the dis
     */
    public DataInputStreamReader(DataInputStream dis){
        inputStream = dis;
    }

    /* (non-Javadoc)
     * @see org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils.shp.ShapeReader#readDouble()
     */
    @Override
    public double readDouble() throws IOException {

        return inputStream.readDouble();
    }

    /* (non-Javadoc)
     * @see org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils.shp.ShapeReader#readInt()
     */
    @Override
    public int readInt() throws IOException {
        return inputStream.readInt();
    }

    /* (non-Javadoc)
     * @see org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils.shp.ShapeReader#read(byte[])
     */
    @Override
    public void read(byte[] bytes) throws IOException {
        inputStream.read(bytes);
    }

    /* (non-Javadoc)
     * @see org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils.shp.ShapeReader#read(byte[], int, int)
     */
    @Override
    public void read(byte[] bytes, int offset, int len) throws IOException {
        inputStream.read(bytes, offset, len);
    }

    /* (non-Javadoc)
     * @see org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils.shp.ShapeReader#read(double[])
     */
    @Override
    public void read(double[] doubles) throws IOException {
        byte[] bytes = new byte[doubles.length * ShapeFileConst.DOUBLE_LENGTH];
        inputStream.readFully(bytes);
        DoubleBuffer doubleBuffer = ByteBuffer.wrap(bytes).asDoubleBuffer();
        doubleBuffer.get(doubles);
    }

    /* (non-Javadoc)
     * @see org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils.shp.ShapeReader#skip(int)
     */
    @Override
    public void skip(int n) throws IOException {
        inputStream.skip(n);
    }
}
