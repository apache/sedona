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
import java.nio.ByteBuffer;

public abstract class ShapeParser implements Serializable {

    /** The geometry factory. */
    protected final GeometryFactory geometryFactory;

    /**
     * Instantiates a new shape parser.
     *
     * @param geometryFactory the geometry factory
     */
    public ShapeParser(GeometryFactory geometryFactory) {
        this.geometryFactory = geometryFactory;
    }

    /**
     * parse the shape to a geometry.
     *
     * @param buffer the buffer
     * @return the geometry
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public abstract Geometry parseShape(ByteBuffer buffer) throws IOException;
}
