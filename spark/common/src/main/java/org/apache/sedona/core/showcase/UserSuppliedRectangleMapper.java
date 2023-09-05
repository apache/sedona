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

package org.apache.sedona.core.showcase;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.io.WKTReader;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

// TODO: Auto-generated Javadoc

/**
 * The Class UserSuppliedRectangleMapper.
 */
public class UserSuppliedRectangleMapper
        implements FlatMapFunction<Iterator<String>, Object>
{

    /**
     * The spatial object.
     */
    Geometry spatialObject = null;

    /**
     * The multi spatial objects.
     */
    MultiPolygon multiSpatialObjects = null;

    /**
     * The fact.
     */
    GeometryFactory fact = new GeometryFactory();

    /**
     * The line split list.
     */
    List<String> lineSplitList;

    /**
     * The coordinates list.
     */
    ArrayList<Coordinate> coordinatesList;

    /**
     * The coordinates.
     */
    Coordinate[] coordinates;

    /**
     * The linear.
     */
    LinearRing linear;

    /**
     * The actual end offset.
     */
    int actualEndOffset;

    @Override
    public Iterator<Object> call(Iterator<String> stringIterator)
            throws Exception
    {
        List result = new ArrayList<Envelope>();
        while (stringIterator.hasNext()) {
            String line = stringIterator.next();
            Geometry spatialObject = null;
            MultiPolygon multiSpatialObjects = null;
            List<String> lineSplitList;
            lineSplitList = Arrays.asList(line.split("\t"));
            String newLine = lineSplitList.get(0).replace("\"", "");
            WKTReader wktreader = new WKTReader();
            spatialObject = wktreader.read(newLine);
            if (spatialObject instanceof MultiPolygon) {
                multiSpatialObjects = (MultiPolygon) spatialObject;
                for (int i = 0; i < multiSpatialObjects.getNumGeometries(); i++) {
                    spatialObject = multiSpatialObjects.getGeometryN(i);
                    result.add(spatialObject.getEnvelopeInternal());
                }
            }
            else {
                result.add(spatialObject.getEnvelopeInternal());
            }
        }
        return result.iterator();
    }
}
