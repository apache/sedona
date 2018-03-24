/**
 * FILE: PolyLineParser.java
 * PATH: org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils.shp.PolyLineParser.java
 * Copyright (c) 2015-2017 GeoSpark Development Team
 * All rights reserved.
 */
package org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils.shp;

import com.vividsolutions.jts.geom.CoordinateSequence;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LineString;

import java.io.IOException;

import static org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils.shp.ShapeFileConst.DOUBLE_LENGTH;

public class PolyLineParser extends ShapeParser {

    /**
     * create a parser that can abstract a MultiPolyline from input source with given GeometryFactory.
     *
     * @param geometryFactory the geometry factory
     */
    public PolyLineParser(GeometryFactory geometryFactory) {
        super(geometryFactory);
    }

    /**
     * abstract a Polyline shape.
     *
     * @param reader the reader
     * @return the geometry
     * @throws IOException Signals that an I/O exception has occurred.
     */
    @Override
    public Geometry parseShape(ShapeReader reader) {
        reader.skip(4 * DOUBLE_LENGTH);
        int numParts = reader.readInt();
        int numPoints = reader.readInt();

        int[] offsets = readOffsets(reader, numParts, numPoints);

        LineString[] lines = new LineString[numParts];
        for(int i = 0; i < numParts; ++i){
            int readScale = offsets[i+1] - offsets[i];
            CoordinateSequence csString = readCoordinates(reader, readScale);
            lines[i] = geometryFactory.createLineString(csString);
        }

        if (numParts == 1) {
            return lines[0];
        }

        return geometryFactory.createMultiLineString(lines);
    }
}
