/**
 * FILE: ShapeParser.java
 * PATH: org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils.shp.ShapeParser.java
 * Copyright (c) 2015-2017 GeoSpark Development Team
 * All rights reserved.
 */
package org.datasyslab.geospark.formatMapper.shapefileParser.parseUtils.shp;

import com.vividsolutions.jts.geom.CoordinateSequence;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;

import java.io.IOException;
import java.io.Serializable;

public abstract class ShapeParser
        implements Serializable
{

    /**
     * The geometry factory.
     */
    protected final GeometryFactory geometryFactory;

    /**
     * Instantiates a new shape parser.
     *
     * @param geometryFactory the geometry factory
     */
    protected ShapeParser(GeometryFactory geometryFactory)
    {
        this.geometryFactory = geometryFactory;
    }

    /**
     * parse the shape to a geometry.
     *
     * @param reader the reader
     * @return the geometry
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public abstract Geometry parseShape(ShapeReader reader);

    /**
     * read numPoints of coordinates from input source.
     *
     * @param reader the reader
     * @param numPoints the num points
     * @return the coordinate sequence
     * @throws IOException Signals that an I/O exception has occurred.
     */
    protected CoordinateSequence readCoordinates(ShapeReader reader, int numPoints)
    {
        CoordinateSequence coordinateSequence = geometryFactory.getCoordinateSequenceFactory().create(numPoints, 2);
        for (int i = 0; i < numPoints; ++i) {
            coordinateSequence.setOrdinate(i, 0, reader.readDouble());
            coordinateSequence.setOrdinate(i, 1, reader.readDouble());
        }
        return coordinateSequence;
    }

    protected int[] readOffsets(ShapeReader reader, int numParts, int maxOffset)
    {
        int[] offsets = new int[numParts + 1];
        for (int i = 0; i < numParts; ++i) {
            offsets[i] = reader.readInt();
        }
        offsets[numParts] = maxOffset;

        return offsets;
    }
}
