package org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils.shp;

import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.util.hash.Hash;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by zongsizhang on 5/3/17.
 */
public interface ShapeFileConst {

    /**
     * Consts for .shp file
     */
    public static final int EXPECT_FILE_CODE = 9994;
    public static final int EXPECT_FILE_VERSION = 1000;
    public static final int HEAD_FILE_LENGTH_16BIT = 50;

    public static final int HEAD_EMPTY_NUM = 5;
    public static final int HEAD_BOX_NUM = 8;

    public static final int INT_LENGTH = 4;
    public static final int DOUBLE_LENGTH = 8;


    /**
     * Consts for .dbf file
     */
    public static final byte FIELD_DESCRIPTOR_TERMINATOR = 0x0d;
    public static final byte FIELD_NAME_LENGTH = 11;
    public static final byte RECORD_DELETE_FLAG = 0x2A;
    public static final byte FILE_END_FLAG = 0x1A;
    public static final byte RECORD_EXIST_FLAG = 0x20;
}
