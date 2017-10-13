package org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils.shp;

import com.esotericsoftware.kryo.io.Input;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class ShapeReaderFactory {

    public static ShapeReader fromByteBuffer(final ByteBuffer buffer) {
        final ByteBuffer leBuffer = buffer.slice().order(ByteOrder.LITTLE_ENDIAN);
        return new ShapeReader() {
            @Override
            public int readInt() {
                return leBuffer.getInt();
            }

            @Override
            public double readDouble() {
                return leBuffer.getDouble();
            }

            @Override
            public byte readByte() {
                return leBuffer.get();
            }

            @Override
            public void skip(int numBytes) {
                leBuffer.position(leBuffer.position() + numBytes);
            }
        };
    }

    public static ShapeReader fromInput(final Input input) {
        return new ShapeReader() {
            @Override
            public int readInt() {
                return toByteBuffer(input, 4).getInt();
            }

            @Override
            public double readDouble() {
                return toByteBuffer(input, 8).getDouble();
            }

            @Override
            public byte readByte() {
                return input.readByte();
            }

            @Override
            public void skip(int numBytes) {
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
