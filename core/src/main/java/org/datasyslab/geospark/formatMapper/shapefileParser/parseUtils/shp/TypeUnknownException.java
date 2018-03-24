/**
 * FILE: TypeUnknownException.java
 * PATH: org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils.shp.TypeUnknownException.java
 * Copyright (c) 2015-2017 GeoSpark Development Team
 * All rights reserved.
 */
package org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils.shp;

// TODO: Auto-generated Javadoc
/**
 * The Class TypeUnknownException.
 */
public class TypeUnknownException extends RuntimeException {

    /**
     * create an exception indicates that the shape type number we get from .shp file is valid
     *
     * @param typeID the type ID
     */
    public TypeUnknownException(int typeID) {
        super("Unknown shape type " + typeID);
    }
}
