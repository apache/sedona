/**
 * FILE: PolygonFormatMapper.java
 * PATH: org.datasyslab.geospark.formatMapper.PolygonFormatMapper.java
 * Copyright (c) 2015-2017 GeoSpark Development Team
 * All rights reserved.
 */
package org.datasyslab.geospark.formatMapper;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.geom.MultiPolygon;
import com.vividsolutions.jts.geom.Polygon;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.datasyslab.geospark.enums.FileDataSplitter;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class PolygonFormatMapper extends FormatMapper
    implements FlatMapFunction<Iterator<String>, Polygon> {
 
	
    /**
     * Instantiates a new polygon format mapper.
     *
     * @param Splitter the splitter
     * @param carryInputData the carry input data
     */
    public PolygonFormatMapper(FileDataSplitter Splitter, boolean carryInputData) {
		super(Splitter, carryInputData);
	}

	/**
	 * Instantiates a new polygon format mapper.
	 *
	 * @param startOffset the start offset
	 * @param endOffset the end offset
	 * @param Splitter the splitter
	 * @param carryInputData the carry input data
	 */
	public PolygonFormatMapper(Integer startOffset, Integer endOffset, FileDataSplitter Splitter,
			boolean carryInputData) {
		super(startOffset, endOffset, Splitter, carryInputData);
	}

    @Override
    public Iterator<Polygon> call(Iterator<String> stringIterator) throws Exception {
        List<Polygon> result = new ArrayList<>();
        while (stringIterator.hasNext()) {
            String line = stringIterator.next();
            switch (splitter) {
                case CSV:
                case TSV: {
                    Coordinate[] coordinates = readCoordinates(line);
                    LinearRing linearRing = factory.createLinearRing(coordinates);
                    Polygon polygon = factory.createPolygon(linearRing);
                    if (this.carryInputData) {
                        polygon.setUserData(line);
                    }
                    result.add(polygon);
                    break;
                }
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
            }
        }
        return result.iterator();
    }

    private void addGeometry(Geometry geometry, List<Polygon> result) {
        if (geometry instanceof MultiPolygon) {
            addMultiGeometry((MultiPolygon) geometry, result);
        } else {
            result.add((Polygon) geometry);
        }
    }
}