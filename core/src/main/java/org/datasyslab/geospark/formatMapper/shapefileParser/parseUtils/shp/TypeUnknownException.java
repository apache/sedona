package org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils.shp;

/**
 * Created by zongsizhang on 6/20/17.
 */
public class TypeUnknownException extends Exception{
    public TypeUnknownException(int typeID) {
        super("Unknown shape type " + typeID);
    }
}
