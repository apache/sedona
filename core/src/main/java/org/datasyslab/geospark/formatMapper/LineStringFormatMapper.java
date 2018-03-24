/**
 * FILE: LineStringFormatMapper.java
 * PATH: org.datasyslab.geospark.formatMapper.LineStringFormatMapper.java
 * Copyright (c) 2015-2017 GeoSpark Development Team
 * All rights reserved.
 */
package org.datasyslab.geospark.formatMapper;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.MultiLineString;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.datasyslab.geospark.enums.FileDataSplitter;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class LineStringFormatMapper
        extends FormatMapper
        implements FlatMapFunction<Iterator<String>, LineString>
{

    /**
     * Instantiates a new line string format mapper.
     *
     * @param Splitter the splitter
     * @param carryInputData the carry input data
     */
    public LineStringFormatMapper(FileDataSplitter Splitter, boolean carryInputData)
    {
        super(Splitter, carryInputData);
    }

    /**
     * Instantiates a new line string format mapper.
     *
     * @param startOffset the start offset
     * @param endOffset the end offset
     * @param Splitter the splitter
     * @param carryInputData the carry input data
     */
    public LineStringFormatMapper(Integer startOffset, Integer endOffset, FileDataSplitter Splitter,
            boolean carryInputData)
    {
        super(startOffset, endOffset, Splitter, carryInputData);
    }

    @Override
    public Iterator<LineString> call(Iterator<String> stringIterator)
            throws Exception
    {
        List<LineString> result = new ArrayList<>();
        while (stringIterator.hasNext()) {
            String line = stringIterator.next();
            switch (splitter) {
                case GEOJSON: {
                    Geometry geometry = readGeoJSON(line);
                    addGeometry(geometry, result);
                    break;
                }
                case WKT: {
                    Geometry geometry = readWkt(line);
                    addGeometry(geometry, result);
                    break;
                }
                default: {
                    Coordinate[] coordinates = readCoordinates(line);
                    LineString lineString = factory.createLineString(coordinates);
                    if (this.carryInputData) {
                        lineString.setUserData(line);
                    }
                    result.add(lineString);
                    break;
                }
            }
        }
        return result.iterator();
    }

    private void addGeometry(Geometry geometry, List<LineString> result)
    {
        if (geometry instanceof MultiLineString) {
            addMultiGeometry((MultiLineString) geometry, result);
        }
        else {
            result.add((LineString) geometry);
        }
    }
}