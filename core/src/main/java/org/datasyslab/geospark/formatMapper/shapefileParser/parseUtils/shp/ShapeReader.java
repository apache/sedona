package org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils.shp;

public interface ShapeReader
{

    int readInt();

    double readDouble();

    byte readByte();

    void skip(int numBytes);
}
