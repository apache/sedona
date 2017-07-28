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

public class DataInputStreamReader extends ShapeReader {

    private DataInputStream inputStream = null;

    public DataInputStreamReader(DataInputStream dis){
        inputStream = dis;
    }

    @Override
    public double readDouble() throws IOException {

        return inputStream.readDouble();
    }

    @Override
    public int readInt() throws IOException {
        byte[] intbytes = new byte[ShapeFileConst.INT_LENGTH];
        this.read(intbytes);

        return ByteBuffer.wrap(intbytes).getInt();
    }

    @Override
    public void read(byte[] bytes) throws IOException {
        inputStream.readFully(bytes);
    }

    @Override
    public void read(byte[] bytes, int offset, int len) throws IOException {
        inputStream.readFully(bytes, offset, len);
    }

    @Override
    public void read(double[] doubles) throws IOException {
        byte[] bytes = new byte[doubles.length * ShapeFileConst.DOUBLE_LENGTH];
        inputStream.readFully(bytes);
        DoubleBuffer doubleBuffer = ByteBuffer.wrap(bytes).asDoubleBuffer();
        doubleBuffer.get(doubles);
    }

    @Override
    public void skip(int n) throws IOException {
        inputStream.skip(n);
    }
}
