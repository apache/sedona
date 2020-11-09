/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.sedona.core.formatMapper.shapefileParser.parseUtils.shp;

import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;

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
