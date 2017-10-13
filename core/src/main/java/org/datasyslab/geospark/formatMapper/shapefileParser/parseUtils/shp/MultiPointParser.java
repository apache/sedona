/**
 * FILE: MultiPointParser.java
 * PATH: org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils.shp.MultiPointParser.java
 * Copyright (c) 2015-2017 GeoSpark Development Team
 * All rights reserved.
 */
package org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils.shp;

import com.vividsolutions.jts.geom.CoordinateSequence;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.MultiPoint;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils.shp.ShapeFileConst.DOUBLE_LENGTH;

public class MultiPointParser extends ShapeParser {

    /**
     * create a parser that can abstract a MultiPoint from input source with given GeometryFactory.
     *
     * @param geometryFactory the geometry factory
     */
    public MultiPointParser(GeometryFactory geometryFactory) {
        super(geometryFactory);
    }

    /**
     * abstract a MultiPoint shape.
     *
     * @param buffer the reader
     * @return the geometry
     * @throws IOException Signals that an I/O exception has occurred.
     */
    @Override
    public Geometry parseShape(ByteBuffer buffer) throws IOException {
        buffer.position(buffer.position() + 4 * DOUBLE_LENGTH);
        int numPoints = buffer.getInt();
        CoordinateSequence coordinateSequence = ShpParseUtil.readCoordinates(buffer, numPoints, geometryFactory);
        MultiPoint multiPoint = geometryFactory.createMultiPoint(coordinateSequence);
        return multiPoint;
    }
}
