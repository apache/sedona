package org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.DoubleBuffer;

/**
 * Created by zongsizhang on 6/19/17.
 */
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
        return inputStream.readInt();
    }

    @Override
    public void read(byte[] bytes) throws IOException {
        inputStream.read(bytes);
    }

    @Override
    public void read(byte[] bytes, int offset, int len) throws IOException {
        inputStream.read(bytes, offset, len);
    }

    @Override
    public void read(double[] doubles) throws IOException {
        byte[] bytes = new byte[doubles.length * DOUBLE_LENGTH];
        inputStream.readFully(bytes);
        DoubleBuffer doubleBuffer = ByteBuffer.wrap(bytes).asDoubleBuffer();
        doubleBuffer.get(doubles);
    }

    @Override
    public void skip(int n) throws IOException {
        inputStream.skip(n);
    }
}
