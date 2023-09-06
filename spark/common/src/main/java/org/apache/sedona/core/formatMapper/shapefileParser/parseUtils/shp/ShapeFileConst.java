/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.sedona.core.formatMapper.shapefileParser.parseUtils.shp;

// TODO: Auto-generated Javadoc

/**
 * The Interface ShapeFileConst.
 */
public interface ShapeFileConst
{

    /**
     * Consts for .shp file
     */
    int EXPECT_FILE_CODE = 9994;

    /**
     * The Constant EXPECT_FILE_VERSION.
     */
    int EXPECT_FILE_VERSION = 1000;

    /**
     * The Constant HEAD_FILE_LENGTH_16BIT.
     */
    int HEAD_FILE_LENGTH_16BIT = 50;

    /**
     * The Constant HEAD_EMPTY_NUM.
     */
    int HEAD_EMPTY_NUM = 5;

    /**
     * The Constant HEAD_BOX_NUM.
     */
    int HEAD_BOX_NUM = 8;

    /**
     * The Constant INT_LENGTH.
     */
    int INT_LENGTH = 4;

    /**
     * The Constant DOUBLE_LENGTH.
     */
    int DOUBLE_LENGTH = 8;

    /**
     * Consts for .dbf file
     */
    byte FIELD_DESCRIPTOR_TERMINATOR = 0x0d;

    /**
     * The Constant FIELD_NAME_LENGTH.
     */
    byte FIELD_NAME_LENGTH = 11;

    /**
     * The Constant RECORD_DELETE_FLAG.
     */
    byte RECORD_DELETE_FLAG = 0x2A;

    /**
     * The Constant FILE_END_FLAG.
     */
    byte FILE_END_FLAG = 0x1A;

    /**
     * The Constant RECORD_EXIST_FLAG.
     */
    byte RECORD_EXIST_FLAG = 0x20;
}
