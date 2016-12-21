/**
 * FILE: PointFormatMapper.java
 * PATH: org.datasyslab.geospark.formatMapper.PointFormatMapper.java
 * Copyright (c) 2016 Arizona State University Data Systems Lab.
 * All rights reserved.
 */
package org.datasyslab.geospark.formatMapper;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.function.Function;
import org.datasyslab.geospark.enums.FileDataSplitter;
import org.wololo.jts2geojson.GeoJSONReader;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.io.WKTReader;

// TODO: Auto-generated Javadoc
/**
 * The Class PointFormatMapper.
 */
public class PointFormatMapper implements Serializable, Function<String, Object> {
    
    /** The offset. */
    Integer offset = 0;
    
    /** The splitter. */
    FileDataSplitter splitter = FileDataSplitter.CSV;

    /**
     * Instantiates a new point format mapper.
     *
     * @param Offset the offset
     * @param Splitter the splitter
     */
    public PointFormatMapper(Integer Offset, FileDataSplitter Splitter) {
        this.offset = Offset;
        this.splitter = Splitter;
    }

    /**
     * Instantiates a new point format mapper.
     *
     * @param Splitter the splitter
     */
    public PointFormatMapper( FileDataSplitter Splitter) {
        this.offset = 0;
        this.splitter = Splitter;
    }

    /* (non-Javadoc)
     * @see org.apache.spark.api.java.function.Function#call(java.lang.Object)
     */
    public Point call(String line) throws Exception {
        Point point = null;
        GeometryFactory fact = new GeometryFactory();
        List<String> lineSplitList;
        Coordinate coordinate;
        switch (splitter) {
            case CSV:
                lineSplitList = Arrays.asList(line.split(splitter.getDelimiter()));
                coordinate= new Coordinate(Double.parseDouble(lineSplitList.get(0 + this.offset)),
                        Double.parseDouble(lineSplitList.get(1 + this.offset)));
                point = fact.createPoint(coordinate);
                point.setUserData(line);
                break;
            case TSV:
                lineSplitList = Arrays.asList(line.split(splitter.getDelimiter()));
                coordinate = new Coordinate(Double.parseDouble(lineSplitList.get(0 + this.offset)),
                        Double.parseDouble(lineSplitList.get(1 + this.offset)));
                point = fact.createPoint(coordinate);
                point.setUserData(line);
                break;
            case GEOJSON:
                GeoJSONReader reader = new GeoJSONReader();
                point = (Point)reader.read(line);
                point.setUserData(line);
                break;
            case WKT:
            	lineSplitList = Arrays.asList(line.split(splitter.getDelimiter()));
                WKTReader wktreader = new WKTReader();
                Envelope envelope=wktreader.read(lineSplitList.get(offset)).getEnvelopeInternal();
                coordinate = new Coordinate (envelope.getMinX(),envelope.getMinY());
                point = fact.createPoint(coordinate);
                coordinate = new Coordinate (85.01,34.01);
                point = fact.createPoint(coordinate);
                point.setUserData(line);
                break;
            default:
                throw new Exception("Input type not recognized, ");
        }
        return point;
    }
}
