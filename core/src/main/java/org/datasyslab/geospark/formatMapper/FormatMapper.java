/**
 * FILE: FormatMapper.java
 * PATH: org.datasyslab.geospark.formatMapper.FormatMapper.java
 * Copyright (c) 2015-2017 GeoSpark Development Team
 * All rights reserved.
 */
package org.datasyslab.geospark.formatMapper;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryCollection;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;
import org.datasyslab.geospark.enums.FileDataSplitter;
import org.wololo.geojson.Feature;
import org.wololo.geojson.GeoJSONFactory;
import org.wololo.jts2geojson.GeoJSONReader;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.List;

public class FormatMapper implements Serializable {

    /** The start offset. */
    protected final int startOffset;

    /** The end offset. */
    /* If the initial value is negative, GeoSpark will consider each field as a spatial attribute if the target object is LineString or Polygon. */
    protected final int endOffset;
    
    /** The splitter. */
    protected final FileDataSplitter splitter;

    /** The carry input data. */
    protected final boolean carryInputData;
    
    /** The factory. */
    transient protected GeometryFactory factory = new GeometryFactory();

    transient protected GeoJSONReader geoJSONReader = new GeoJSONReader();

    transient protected WKTReader wktReader = new WKTReader();

    /**
     * Instantiates a new format mapper.
     *
     * @param startOffset the start offset
     * @param endOffset the end offset
     * @param splitter the splitter
     * @param carryInputData the carry input data
     */
    public FormatMapper(int startOffset, int endOffset, FileDataSplitter splitter, boolean carryInputData) {
        this.startOffset = startOffset;
        this.endOffset = endOffset;
        this.splitter = splitter;
        this.carryInputData = carryInputData;
    }

    /**
     * Instantiates a new format mapper.
     *
     * @param splitter the splitter
     * @param carryInputData the carry input data
     */
    public FormatMapper(FileDataSplitter splitter, boolean carryInputData) {
        this(0, -1, splitter, carryInputData);
    }

    private void readObject(ObjectInputStream inputStream) throws IOException, ClassNotFoundException {
        inputStream.defaultReadObject();
        factory = new GeometryFactory();
        wktReader = new WKTReader();
        geoJSONReader = new GeoJSONReader();
    }

    public Geometry readGeoJSON(String geoJson) {
        final Geometry geometry;
        if (geoJson.contains("Feature")) {
            Feature feature = (Feature) GeoJSONFactory.create(geoJson);
            geometry = geoJSONReader.read(feature.getGeometry());
        } else {
            geometry = geoJSONReader.read(geoJson);
        }

        if (carryInputData) {
            geometry.setUserData(geoJson);
        }
        return geometry;
    }

    public Geometry readWkt(String line) throws ParseException {
        final String[] columns = line.split(splitter.getDelimiter());
        final Geometry geometry = wktReader.read(columns[this.startOffset]);
        if (carryInputData) {
            geometry.setUserData(line);
        }
        return geometry;
    }

    public Coordinate[] readCoordinates(String line) {
        final String[] columns = line.split(splitter.getDelimiter());
        final int actualEndOffset = this.endOffset >= 0 ? this.endOffset : (columns.length - 1);
        final Coordinate[] coordinates = new Coordinate[(actualEndOffset - startOffset + 1)/2];
        for (int i = this.startOffset; i <= actualEndOffset; i += 2) {
            coordinates[i/2] = new Coordinate(Double.parseDouble(columns[i]), Double.parseDouble(columns[i+1]));
        }
        return coordinates;
    }

    public <T extends Geometry> void addMultiGeometry(GeometryCollection multiGeometry, List<T> result) {
        for (int i=0; i<multiGeometry.getNumGeometries(); i++) {
            T geometry = (T) multiGeometry.getGeometryN(i);
            geometry.setUserData(multiGeometry.getUserData());
            result.add(geometry);
        }
    }
}
