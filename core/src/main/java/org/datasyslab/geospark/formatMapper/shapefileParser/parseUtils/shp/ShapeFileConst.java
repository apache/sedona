/*
 * FILE: ShapeFileConst
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

// TODO: Auto-generated Javadoc

/**
 * The Interface ShapeFileConst.
 */
public interface ShapeFileConst
{

    /**
     * Consts for .shp file
     */
    public static final int EXPECT_FILE_CODE = 9994;

    /**
     * The Constant EXPECT_FILE_VERSION.
     */
    public static final int EXPECT_FILE_VERSION = 1000;

    /**
     * The Constant HEAD_FILE_LENGTH_16BIT.
     */
    public static final int HEAD_FILE_LENGTH_16BIT = 50;

    /**
     * The Constant HEAD_EMPTY_NUM.
     */
    public static final int HEAD_EMPTY_NUM = 5;

    /**
     * The Constant HEAD_BOX_NUM.
     */
    public static final int HEAD_BOX_NUM = 8;

    /**
     * The Constant INT_LENGTH.
     */
    public static final int INT_LENGTH = 4;

    /**
     * The Constant DOUBLE_LENGTH.
     */
    public static final int DOUBLE_LENGTH = 8;

    /**
     * Consts for .dbf file
     */
    public static final byte FIELD_DESCRIPTOR_TERMINATOR = 0x0d;

    /**
     * The Constant FIELD_NAME_LENGTH.
     */
    public static final byte FIELD_NAME_LENGTH = 11;

    /**
     * The Constant RECORD_DELETE_FLAG.
     */
    public static final byte RECORD_DELETE_FLAG = 0x2A;

    /**
     * The Constant FILE_END_FLAG.
     */
    public static final byte FILE_END_FLAG = 0x1A;

    /**
     * The Constant RECORD_EXIST_FLAG.
     */
    public static final byte RECORD_EXIST_FLAG = 0x20;
}
