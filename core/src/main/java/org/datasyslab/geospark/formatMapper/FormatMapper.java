/*
 * FILE: FormatMapper
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
package org.datasyslab.geospark.formatMapper;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryCollection;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.MultiLineString;
import com.vividsolutions.jts.geom.MultiPoint;
import com.vividsolutions.jts.geom.MultiPolygon;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKBReader;
import com.vividsolutions.jts.io.WKTReader;
import com.vividsolutions.jts.operation.valid.IsValidOp;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.datasyslab.geospark.enums.FileDataSplitter;
import org.datasyslab.geospark.enums.GeometryType;
import org.wololo.geojson.Feature;
import org.wololo.geojson.GeoJSONFactory;
import org.wololo.jts2geojson.GeoJSONReader;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.*;

import static loci.formats.LogTools.print;
import static org.datasyslab.geospark.enums.FileDataSplitter.GEOJSON;

public class FormatMapper<T extends Geometry>
        implements Serializable, FlatMapFunction<Iterator<String>, T>
{

    /**
     * The start offset.
     */
    protected final int startOffset;

    /**
     * The end offset.
     */
    /* If the initial value is negative, GeoSpark will consider each field as a spatial attribute if the target object is LineString or Polygon. */
    protected final int endOffset;

    /**
     * The splitter.
     */
    protected final FileDataSplitter splitter;

    /**
     * The carry input data.
     */
    protected final boolean carryInputData;

    /**
     * Non-spatial attributes in each input row will be concatenated to a tab separated string
     */
    protected String otherAttributes = "";

    protected GeometryType geometryType = null;

    /**
     *  Allow mapping of invalid geometries.
     */
    boolean allowTopologicallyInvalidGeometries;

    /**
     *  Crash on syntactically invalid geometries or skip them.
     */
    boolean skipSyntacticallyInvalidGeometries;
    /**
     * The factory.
     */
    transient protected GeometryFactory factory = new GeometryFactory();

    transient protected GeoJSONReader geoJSONReader = new GeoJSONReader();

    transient protected WKTReader wktReader = new WKTReader();
    // For some unknown reasons, the wkb reader cannot be used in transient variable like the wkt reader.

    final static Logger logger = Logger.getLogger(FormatMapper.class);
    /**
     * Instantiates a new format mapper.
     *
     * @param startOffset the start offset
     * @param endOffset the end offset
     * @param splitter the splitter
     * @param carryInputData the carry input data
     */
    public FormatMapper(int startOffset, int endOffset, FileDataSplitter splitter, boolean carryInputData, GeometryType geometryType)
    {
        this.startOffset = startOffset;
        this.endOffset = endOffset;
        this.splitter = splitter;
        this.carryInputData = carryInputData;
        this.geometryType = geometryType;
        this.allowTopologicallyInvalidGeometries = true;
        this.skipSyntacticallyInvalidGeometries = false;
        // Only the following formats are allowed to use this format mapper because each input has the geometry type definition
        if (geometryType == null)
        {
            assert splitter == FileDataSplitter.WKB || splitter == FileDataSplitter.WKT || splitter == GEOJSON;
        }
    }

    /**
     * Instantiates a new format mapper. This is extensively used in GeoSparkSQL.
     * @param splitter
     * @param carryInputData
     */
    public FormatMapper(FileDataSplitter splitter, boolean carryInputData)
    {
        this(0,-1,splitter,carryInputData, null);
    }


    /**
     * This format mapper is used in GeoSparkSQL.
     * @param splitter
     * @param carryInputData
     * @param geometryType
     */
    public FormatMapper(FileDataSplitter splitter, boolean carryInputData, GeometryType geometryType)
    {
        this(0, -1, splitter, carryInputData, geometryType);
    }

    private void readObject(ObjectInputStream inputStream)
            throws IOException, ClassNotFoundException
    {
        inputStream.defaultReadObject();
        factory = new GeometryFactory();
        wktReader = new WKTReader();
        geoJSONReader = new GeoJSONReader();
    }

    private void handleNonSpatialDataToGeometry(Geometry geometry, List<String> splitedGeometryData)
    {
        LinkedList<String> splitedGeometryDataList = new LinkedList<String>(splitedGeometryData);
        if (carryInputData) {
            if (this.splitter != GEOJSON){
                //remove spatial data position
                splitedGeometryDataList.remove(this.startOffset);
            }
            geometry.setUserData(String.join("\t", splitedGeometryDataList));
        }
    }

    public Geometry readGeoJSON(String geoJson)
    {
        final Geometry geometry;
        if (geoJson.contains("Feature")) {
            Feature feature = (Feature) GeoJSONFactory.create(geoJson);
            ArrayList<String> nonSpatialData = new ArrayList<>();
            Map<String, Object> featurePropertiesproperties = feature.getProperties();
            if (feature.getId()!=null)
            {
                nonSpatialData.add(feature.getId().toString());
            }
            if (featurePropertiesproperties!=null)
            {
                for (Object property:featurePropertiesproperties.values()
                ) {
                    nonSpatialData.add(property.toString());
                }
            }
            geometry = geoJSONReader.read(feature.getGeometry());
            handleNonSpatialDataToGeometry(geometry, nonSpatialData);
        }
        else {
            geometry = geoJSONReader.read(geoJson);
        }
        return geometry;
    }

    private static List<String> readGeoJsonPropertyNames(String geoJson){
        if (geoJson.contains("Feature") || geoJson.contains("feature") || geoJson.contains("FEATURE")) {
            if (geoJson.contains("properties")) {
                Feature feature = (Feature) GeoJSONFactory.create(geoJson);
                return new ArrayList(feature.getProperties().keySet());
            }
        }
        logger.warn("[GeoSpark] The GeoJSON file doesn't have feature properties");
        return null;
    }

    public static List<String> readPropertyNames(String geoString, FileDataSplitter fileType) {
        switch (fileType){
            case GEOJSON:
              return readGeoJsonPropertyNames(geoString);
              default:
                  return null;
        }
    }

    public Geometry readWkt(String line)
            throws ParseException
    {
        final String[] columns = line.split(splitter.getDelimiter());
        Geometry geometry = null;

        try {
            geometry = wktReader.read(columns[this.startOffset]);
        }
        catch (Exception e){
            print("[GeoSpark] " + e.getMessage());
        }
        if (geometry == null){
            return null;
        }
        handleNonSpatialDataToGeometry(geometry, Arrays.asList(columns));
        return geometry;
    }

    public Geometry readWkb(String line)
            throws ParseException
    {
        final String[] columns = line.split(splitter.getDelimiter());
        final byte[] aux = WKBReader.hexToBytes(columns[this.startOffset]);
        // For some unknown reasons, the wkb reader cannot be used in transient variable like the wkt reader.
        WKBReader wkbReader = new WKBReader();
        final Geometry geometry = wkbReader.read(aux);
        handleNonSpatialDataToGeometry(geometry, Arrays.asList(columns));

        return geometry;
    }

    public Coordinate[] readCoordinates(String line)
    {
        final String[] columns = line.split(splitter.getDelimiter());
        final int actualEndOffset = this.endOffset >= 0 ? this.endOffset : (this.geometryType == GeometryType.POINT? startOffset+1:columns.length - 1);
        final Coordinate[] coordinates = new Coordinate[(actualEndOffset - startOffset + 1) / 2];
        for (int i = this.startOffset; i <= actualEndOffset; i += 2) {
            coordinates[i / 2] = new Coordinate(Double.parseDouble(columns[i]), Double.parseDouble(columns[i + 1]));
        }
        if (carryInputData)
        {
            boolean firstColumnFlag = true;
            otherAttributes = "";
            for (int i= 0;i<this.startOffset;i++)
            {
                if (firstColumnFlag)
                {
                    otherAttributes += columns[i];
                    firstColumnFlag = false;
                }
                else otherAttributes += "\t" + columns[i];
            }
            for (int i=actualEndOffset+1;i<columns.length;i++)
            {
                if (firstColumnFlag)
                {
                    otherAttributes += columns[i];
                    firstColumnFlag = false;
                }
                else otherAttributes += "\t" + columns[i];
            }
        }
        return coordinates;
    }

    public <T extends Geometry> void addMultiGeometry(GeometryCollection multiGeometry, List<T> result)
    {
        for (int i = 0; i < multiGeometry.getNumGeometries(); i++) {
            T geometry = (T) multiGeometry.getGeometryN(i);
            geometry.setUserData(multiGeometry.getUserData());
            result.add(geometry);
        }
    }

    public Geometry readGeometry(String line)
            throws ParseException
    {
        Geometry geometry = null;
        try {
            switch (this.splitter) {
                case WKT:
                    geometry = readWkt(line);
                    break;
                case WKB:
                    geometry = readWkb(line);
                    break;
                case GEOJSON:
                    geometry = readGeoJSON(line);
                    break;
                default: {
                    if (this.geometryType == null) {
                        throw new IllegalArgumentException("[GeoSpark][FormatMapper] You must specify GeometryType when you use delimiter rather than WKB, WKT or GeoJSON");
                    } else {
                        geometry = createGeometry(readCoordinates(line), geometryType);
                    }
                }
            }

        }
        catch (Exception e){
            print("[GeoSpark] " + e.getMessage());
            if (skipSyntacticallyInvalidGeometries == false){
                throw e;
            }
        }
        if (geometry == null){
            return null;
        }
        if (allowTopologicallyInvalidGeometries == false) {
            IsValidOp isvalidop = new IsValidOp(geometry);
            if (isvalidop.isValid() == false) {
                geometry = null;
            }
        }

        return geometry;
    }

    private Geometry createGeometry(Coordinate[] coordinates, GeometryType geometryType)
    {
        GeometryFactory geometryFactory = new GeometryFactory();
        Geometry geometry = null;
        switch (geometryType) {
            case POINT:
                geometry = geometryFactory.createPoint(coordinates[0]);
                break;
            case POLYGON:
                geometry = geometryFactory.createPolygon(coordinates);
                break;
            case LINESTRING:
                geometry = geometryFactory.createLineString(coordinates);
                break;
            case RECTANGLE:
                // The rectangle mapper reads two coordinates from the input line. The two coordinates are the two on the diagonal.
                assert  coordinates.length == 2;
                Coordinate[] polyCoordinates = new Coordinate[5];
                polyCoordinates[0] = coordinates[0];
                polyCoordinates[1] = new Coordinate(coordinates[0].x, coordinates[1].y);
                polyCoordinates[2] = coordinates[1];
                polyCoordinates[3] = new Coordinate(coordinates[1].x, coordinates[0].y);
                polyCoordinates[4] = polyCoordinates[0];
                geometry = factory.createPolygon(polyCoordinates);
                break;
            // Read string to point if no geometry type specified but GeoSpark should never reach here
            default:
                geometry = geometryFactory.createPoint(coordinates[0]);
        }
        if (carryInputData)
        {
            geometry.setUserData(otherAttributes);
        }
        return geometry;
    }

    @Override
    public Iterator<T> call(Iterator<String> stringIterator)
            throws Exception
    {
        List<T> result = new ArrayList<>();
        while (stringIterator.hasNext()) {
            String line = stringIterator.next();
            addGeometry(readGeometry(line), result);
        }
        return result.iterator();
    }

    private void addGeometry(Geometry geometry, List<T> result)
    {
        if (geometry == null) {
            return;
        }
        if (geometry instanceof MultiPoint) {
            addMultiGeometry((MultiPoint) geometry, result);
        }
        else if (geometry instanceof MultiLineString) {
            addMultiGeometry((MultiLineString) geometry, result);
        }
        else if (geometry instanceof MultiPolygon) {
            addMultiGeometry((MultiPolygon) geometry, result);
        }
        else {
            result.add((T) geometry);
        }
    }
}
