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
import org.locationtech.jts.geom.LineString;

import java.io.IOException;

public class PolyLineParser
        extends ShapeParser
{

    /**
     * create a parser that can abstract a MultiPolyline from input source with given GeometryFactory.
     *
     * @param geometryFactory the geometry factory
     */
    public PolyLineParser(GeometryFactory geometryFactory)
    {
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
    public Geometry parseShape(ShapeReader reader)
    {
        reader.skip(4 * ShapeFileConst.DOUBLE_LENGTH);
        int numParts = reader.readInt();
        int numPoints = reader.readInt();

        int[] offsets = readOffsets(reader, numParts, numPoints);

        LineString[] lines = new LineString[numParts];
        for (int i = 0; i < numParts; ++i) {
            int readScale = offsets[i + 1] - offsets[i];
            CoordinateSequence csString = readCoordinates(reader, readScale);
            lines[i] = geometryFactory.createLineString(csString);
        }

        if (numParts == 1) {
            return lines[0];
        }

        return geometryFactory.createMultiLineString(lines);
    }
}
