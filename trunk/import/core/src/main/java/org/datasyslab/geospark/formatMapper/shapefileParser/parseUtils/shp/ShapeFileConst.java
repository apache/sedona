/*
 * FILE: ShapeFileConst
 * Copyright (c) 2015 - 2019 GeoSpark Development Team
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
