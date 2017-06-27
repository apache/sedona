/**
 * FILE: TypeUnknownException.java
 * PATH: org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils.shp.TypeUnknownException.java
 * Copyright (c) 2015-2017 GeoSpark Development Team
 * All rights reserved.
 */
package org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils.shp;

public class TypeUnknownException extends Exception{
    public TypeUnknownException(int typeID) {
        super("Unknown shape type " + typeID);
    }
}
