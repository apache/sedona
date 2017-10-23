/**
 * FILE: RectangleFormatMapper.java
 * PATH: org.datasyslab.geospark.formatMapper.RectangleFormatMapper.java
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

public class RectangleFormatMapper extends FormatMapper
	implements FlatMapFunction<Iterator<String>, Polygon>
{

	/**
	 * Instantiates a new rectangle format mapper.
	 *
	 * @param Splitter the splitter
	 * @param carryInputData the carry input data
	 */
	public RectangleFormatMapper(FileDataSplitter Splitter, boolean carryInputData) {
		super(Splitter, carryInputData);
	}

	/**
	 * Instantiates a new rectangle format mapper.
	 *
	 * @param startOffset the start offset
	 * @param endOffset the end offset
	 * @param Splitter the splitter
	 * @param carryInputData the carry input data
	 */
	public RectangleFormatMapper(Integer startOffset, Integer endOffset, FileDataSplitter Splitter,
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
					String[] columns = line.split(splitter.getDelimiter());
					double x1 = Double.parseDouble(columns[this.startOffset]);
					double x2 = Double.parseDouble(columns[this.startOffset + 2]);
					double y1 = Double.parseDouble(columns[this.startOffset + 1]);
					double y2 = Double.parseDouble(columns[this.startOffset + 3]);

					Coordinate[] coordinates = new Coordinate[5];
					coordinates[0] = new Coordinate(x1, y1);
					coordinates[1] = new Coordinate(x1, y2);
					coordinates[2] = new Coordinate(x2, y2);
					coordinates[3] = new Coordinate(x2, y1);
					coordinates[4] = coordinates[0];

					LinearRing linear = factory.createLinearRing(coordinates);
					Polygon polygon = new Polygon(linear, null, factory);
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
            result.add((Polygon) (geometry.getEnvelope()));
        }
	}
}
