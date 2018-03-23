/*
 * FILE: ShapeParser
 * Copyright (c) 2015 - 2018 GeoSpark Development Team
 *
 * MIT License
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
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
