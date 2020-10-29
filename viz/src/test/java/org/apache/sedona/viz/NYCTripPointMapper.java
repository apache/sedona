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
package org.apache.sedona.viz;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.Point;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

// TODO: Auto-generated Javadoc

/**
 * The Class NYCTripPointMapper.
 */
class NYCTripPointMapper
        implements FlatMapFunction<String, Object>
{

    /**
     * The result.
     */
    List<Point> result = new ArrayList<Point>();

    /* (non-Javadoc)
     * @see org.apache.spark.api.java.function.FlatMapFunction#call(java.lang.Object)
     */
    public Iterator call(String line)
            throws Exception
    {
        try {
            List result = new ArrayList<LineString>();
            Geometry spatialObject = null;
            List<String> lineSplitList = Arrays.asList(line.split(","));
            Coordinate coordinate = new Coordinate(Double.parseDouble(lineSplitList.get(5)), Double.parseDouble(lineSplitList.get(6)));
            GeometryFactory geometryFactory = new GeometryFactory();
            result.add(geometryFactory.createPoint(coordinate));
            return result.iterator();
        }
        catch (NumberFormatException e) {
            return result.iterator();
        }
        catch (ArrayIndexOutOfBoundsException e) {
            System.out.println(line);
            //e.printStackTrace();
        }

        return result.iterator();
    }
}

