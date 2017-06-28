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

public class MultiPointParser extends ShapeParser {

    /**
     * create a parser that can abstract a MultiPolyline from input source with given GeometryFactory
     * @param geometryFactory
     */
    public MultiPointParser(GeometryFactory geometryFactory) {
        super(geometryFactory);
    }

    /**
     * abstract a MultiPoint shape
     * @param reader
     * @return
     * @throws IOException
     */
    @Override
    public Geometry parserShape(ShapeReader reader) throws IOException {
        reader.skip(4 * DOUBLE_LENGTH);
        int numPoints = reader.readInt();
        CoordinateSequence coordinateSequence = ShpParseUtil.readCoordinates(reader, numPoints, geometryFactory);
        MultiPoint multiPoint = geometryFactory.createMultiPoint(coordinateSequence);
        return multiPoint;
    }
}
