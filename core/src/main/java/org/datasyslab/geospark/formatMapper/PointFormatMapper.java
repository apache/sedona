/**
 * FILE: PointFormatMapper.java
 * PATH: org.datasyslab.geospark.formatMapper.PointFormatMapper.java
 * Copyright (c) 2015-2017 GeoSpark Development Team
 * All rights reserved.
 */
package org.datasyslab.geospark.formatMapper;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.MultiPoint;
import com.vividsolutions.jts.geom.Point;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.datasyslab.geospark.enums.FileDataSplitter;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class PointFormatMapper extends FormatMapper
    implements FlatMapFunction<Iterator<String>, Point> {


	/**
	 * Instantiates a new point format mapper.
	 *
	 * @param Splitter the splitter
	 * @param carryInputData the carry input data
	 */
	public PointFormatMapper(FileDataSplitter Splitter, boolean carryInputData) {
		super(Splitter, carryInputData);
	}

	/**
	 * Instantiates a new point format mapper.
	 *
	 * @param startOffset the start offset
	 * @param endOffset the end offset
	 * @param Splitter the splitter
	 * @param carryInputData the carry input data
	 */
	public PointFormatMapper(Integer startOffset, Integer endOffset, FileDataSplitter Splitter,
			boolean carryInputData) {
		super(startOffset, endOffset, Splitter, carryInputData);
	}

    @Override
    public Iterator<Point> call(final Iterator<String> stringIterator) throws Exception {
        List<Point> result = new ArrayList<>();
        while (stringIterator.hasNext()) {
            String line = stringIterator.next();
            switch (splitter) {
                case GEOJSON: {
                    Geometry geometry = readGeoJSON(line);
                    addGeometry(geometry, result);
                    break;
                }
                case WKT:
                    Geometry geometry = readWkt(line);
                    addGeometry(geometry, result);
                    break;
                default:
                    String[] columns = line.split(splitter.getDelimiter());
                    Coordinate coordinate = new Coordinate(
                            Double.parseDouble(columns[this.startOffset]),
                            Double.parseDouble(columns[1 + this.startOffset]));
                    Point point = factory.createPoint(coordinate);
                    if (this.carryInputData) {
                        point.setUserData(line);
                    }
                    result.add(point);
                    break;
            }
        }
        return result.iterator();
    }

    private void addGeometry(Geometry geometry, List<Point> result) {
        if (geometry instanceof MultiPoint) {
            addMultiGeometry((MultiPoint) geometry, result);
        } else {
            result.add((Point) geometry);
        }
    }
}
