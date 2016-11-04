package org.datasyslab.geospark.formatMapper;

/**
 * 
 * @author Arizona State University DataSystems Lab
 *
 */

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.function.Function;
import org.datasyslab.geospark.enums.FileDataSplitter;
import org.wololo.jts2geojson.GeoJSONReader;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.io.WKTReader;

public class PolygonFormatMapper implements Function<String, Polygon>, Serializable {
    Integer offset = 0;
    FileDataSplitter splitter = FileDataSplitter.CSV;

    public PolygonFormatMapper(Integer Offset, FileDataSplitter Splitter) {
        this.offset = Offset;
        this.splitter = Splitter;
    }

    public PolygonFormatMapper(FileDataSplitter Splitter) {
        this.offset = 0;
        this.splitter = Splitter;
    }

    public Polygon call(String line) throws Exception {
        Polygon polygon = null;
        GeometryFactory fact = new GeometryFactory();
        List<String> lineSplitList;
        ArrayList<Coordinate> coordinatesList;
        Coordinate[] coordinates;
        LinearRing linear;
        switch (splitter) {
            case CSV:
                lineSplitList = Arrays.asList(line.split(splitter.getSplitter()));
                coordinatesList = new ArrayList<Coordinate>();
                for (int i = this.offset; i < lineSplitList.size(); i+=2) {
                    coordinatesList.add(new Coordinate(Double.parseDouble(lineSplitList.get(i)), Double.parseDouble(lineSplitList.get(i + 1))));
                }
                linear = fact.createLinearRing(coordinatesList.toArray(new Coordinate[coordinatesList.size()]));
                polygon = new Polygon(linear, null, fact);
                polygon.setUserData(line);
                break;
            case TSV:
                lineSplitList = Arrays.asList(line.split(splitter.getSplitter()));
                coordinatesList = new ArrayList<Coordinate>();
                for (int i = this.offset; i < lineSplitList.size(); i = i + 2) {
                    coordinatesList.add(new Coordinate(Double.parseDouble(lineSplitList.get(i)), Double.parseDouble(lineSplitList.get(i + 1))));
                }
                coordinates = new Coordinate[coordinatesList.size()];
                coordinates = coordinatesList.toArray(coordinates);
                linear = fact.createLinearRing(coordinates);
                polygon = new Polygon(linear, null, fact);
                polygon.setUserData(line);
                break;
            case GEOJSON:
                GeoJSONReader reader = new GeoJSONReader();
                polygon = (Polygon) reader.read(line);
                polygon.setUserData(line);
                break;
            case WKT:
            	lineSplitList=Arrays.asList(line.split(splitter.getSplitter()));
                WKTReader wtkreader = new WKTReader();
                polygon = (Polygon) wtkreader.read(lineSplitList.get(offset));
                polygon.setUserData(line);
                break;
            default:
                throw new Exception("Input type not recognized, ");
        }
        return polygon;
    }
}