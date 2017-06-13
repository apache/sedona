package org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils;

/**
 * Created by zongsizhang on 5/13/17.
 */
public class ShapeTypeNotMatchException extends Exception{
    public ShapeTypeNotMatchException(){
        super("Current record's type doesn't match with the type in shapefile head");
    }
}
