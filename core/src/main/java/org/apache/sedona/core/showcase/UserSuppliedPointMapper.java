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
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class UserSuppliedPointMapper
        implements FlatMapFunction<Iterator<String>, Point>
{

    /**
     * The factory.
     */
    private final GeometryFactory factory = new GeometryFactory();

    @Override
    public Iterator<Point> call(Iterator<String> stringIterator)
            throws Exception
    {
        List<Point> result = new ArrayList<Point>();
        while (stringIterator.hasNext()) {
            String line = stringIterator.next();
            try {
                //Split the line by comma
                String[] columns = line.split(",");
                //Remove all quotes in the input line
                String latitudeString = columns[2].replaceAll("\"", "");
                String longitudeString = columns[3].replaceAll("\"", "");
                double latitude = Double.parseDouble(latitudeString);
                double longitude = Double.parseDouble(longitudeString);
                Point point = factory.createPoint(new Coordinate(longitude, latitude));
                result.add(point);
            }
            catch (Exception e) {
                //Get one error. The data probably is dirty. Just skip this line.
            }
        }
        return result.iterator();
    }
}
