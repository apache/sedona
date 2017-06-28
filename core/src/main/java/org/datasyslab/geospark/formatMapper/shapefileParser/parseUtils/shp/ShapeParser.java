/**
 * FILE: ShapeParser.java
 * PATH: org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils.shp.ShapeParser.java
 * Copyright (c) 2015-2017 GeoSpark Development Team
 * All rights reserved.
 */
package org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils.shp;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;

import java.io.IOException;
import java.io.Serializable;

public abstract class ShapeParser implements Serializable, ShapeFileConst{

    protected GeometryFactory geometryFactory = null;

    public ShapeParser(GeometryFactory geometryFactory) {
        this.geometryFactory = geometryFactory;
    }

    /**
     * parse the shape to a geometry
     * @param reader
     */
    public abstract Geometry parserShape(ShapeReader reader) throws IOException;

}
