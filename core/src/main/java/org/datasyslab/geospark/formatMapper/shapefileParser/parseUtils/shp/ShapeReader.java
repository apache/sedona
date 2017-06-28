/**
 * FILE: ShapeReader.java
 * PATH: org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils.shp.ShapeReader.java
 * Copyright (c) 2015-2017 GeoSpark Development Team
 * All rights reserved.
 */
package org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils.shp;

import java.io.IOException;
import java.io.Serializable;

public abstract class ShapeReader implements Serializable, ShapeFileConst{

    /**
     * read a double from source
     * @return
     */
    public abstract double readDouble() throws IOException;

    /**
     * read an integer from source
     * @return
     */
    public abstract int readInt() throws IOException;

    /**
     * fully read an array of byte from source
     * @param bytes
     */
    public abstract void read(byte[] bytes) throws IOException;

    /**
     * read len of bytes from source start at offset.
     * @param bytes
     * @param offset
     * @param len
     */
    public abstract void read(byte[] bytes, int offset, int len) throws IOException;

    /**
     * read len of bytes from source
     * @param doubles
     */
    public abstract void read(double[] doubles) throws IOException;

    /**
     * skip n bytes in source
     * @param n
     */
    public abstract void skip(int n) throws IOException;

}
