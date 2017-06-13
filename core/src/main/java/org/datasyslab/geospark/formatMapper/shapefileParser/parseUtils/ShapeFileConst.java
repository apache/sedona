package org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils;

import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.util.hash.Hash;

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

    // value of shapetypes
    public static final int SHAPETYPE_NULLSHAPE = 0;
    public static final int SHAPETYPE_POINT = 1;
    public static final int SHAPETYPE_POLYLINE = 0;
    public static final int SHAPETYPE_POLYGON = 5;
    public static final int SHAPETYPE_MULTIPOINT = 0;
    public static final int SHAPETYPE_POINTZ = 0;
    public static final int SHAPETYPE_POLYLINEZ = 0;
    public static final int SHAPETYPE_POLYGONZ = 0;
    public static final int SHAPETYPE_MULTIPOINTZ = 0;
    public static final int SHAPETYPE_POINTM = 0;
    public static final int SHAPETYPE_POLYLINEM = 0;
    public static final int SHAPETYPE_POLYGONM = 0;
    public static final int SHAPETYPE_MULTIPOINTM = 0;
    public static final int SHAPETYPE_MULTIPATCH = 0;

    //Content Constants
    public static final long BOUND_BOX_DOUBLE_NUM = 4;


    public static final HashMap<Integer, String> typeClassNamePairs = new HashMap<Integer, String>(){
        {
            put(1, "shapes.PointWritable");
            put(3, "shapes.PolyLineWritable");
            put(5, "shapes.PolygonWritable");
            put(8, "shapes.MultiPointWritable");
        }
    };


    /**
     * Consts for .dbf file
     */
    public static final byte FIELD_DESCRIPTOR_TERMINATOR = 0x0d;
    public static final byte FIELD_NAME_LENGTH = 11;
    public static final byte RECORD_DELETE_FLAG = 0x2A;
    public static final byte FILE_END_FLAG = 0x1A;
    public static final byte RECORD_EXIST_FLAG = 0x20;
}
