/*
 * FILE: UserSuppliedPointMapper
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
package org.datasyslab.geospark.showcase;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import org.apache.spark.api.java.function.FlatMapFunction;

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
