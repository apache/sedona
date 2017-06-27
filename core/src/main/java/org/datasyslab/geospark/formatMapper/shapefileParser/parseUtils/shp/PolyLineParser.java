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

public class PolyLineParser extends ShapeParser{

    public PolyLineParser(GeometryFactory geometryFactory) {
        super(geometryFactory);
    }

    @Override
    public Geometry parserShape(ShapeReader reader) throws IOException {
        reader.skip(4 * DOUBLE_LENGTH);
        int numParts = reader.readInt();
        int numPoints = reader.readInt();
        int[] stringOffsets = new int[numParts+1];
        for(int i = 0;i < numParts; ++i){
            stringOffsets[i] = reader.readInt();
        }
        CoordinateSequence coordinateSequence = ShpParseUtil.readCoordinates(reader, numPoints, geometryFactory);
        stringOffsets[numParts] = numPoints;
        LineString[] lines = new LineString[numParts];
        for(int i = 0;i < numParts; ++i){
            int readScale = stringOffsets[i+1] - stringOffsets[i];
            CoordinateSequence csString = geometryFactory.getCoordinateSequenceFactory().create(readScale,2);
            for(int j = 0;j < readScale; ++j){
                csString.setOrdinate(j, 0, coordinateSequence.getOrdinate(stringOffsets[i]+j, 0));
                csString.setOrdinate(j, 1, coordinateSequence.getOrdinate(stringOffsets[i]+j, 1));
            }
            lines[i] = geometryFactory.createLineString(csString);
        }
        return geometryFactory.createMultiLineString(lines);
    }
}
