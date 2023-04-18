/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.sedona.common.utils;


import org.apache.sedona.common.enums.FileDataSplitter;
import org.apache.sedona.common.enums.GeometryType;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;
import org.locationtech.jts.io.WKTReader;
import org.locationtech.jts.operation.valid.IsValidOp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wololo.geojson.Feature;
import org.wololo.geojson.GeoJSONFactory;
import org.wololo.jts2geojson.GeoJSONReader;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.*;

/**
 * This format mapper is isolated on purpose for the sake of sharing across Spark and Flink
 */
public class FormatUtils<T extends Geometry> implements Serializable {
    final static Logger logger = LoggerFactory.getLogger(FormatUtils.class);
    /**
     * The start offset.
     */
    protected final int startOffset;
    /**
     * The end offset.
     */
    /* If the initial value is negative, Sedona will consider each field as a spatial attribute if the target object is LineString or Polygon. */
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
     * The factory.
     */
    transient protected GeometryFactory factory = new GeometryFactory();
    transient protected GeoJSONReader geoJSONReader = new GeoJSONReader();
    transient protected WKTReader wktReader = new WKTReader();
    /**
     * Allow mapping of invalid geometries.
     */
    public boolean allowTopologicallyInvalidGeometries;
    // For some unknown reasons, the wkb reader cannot be used in transient variable like the wkt reader.
    /**
     * Crash on syntactically invalid geometries or skip them.
     */
    public boolean skipSyntacticallyInvalidGeometries;

    /**
     * Instantiates a new format mapper.
     *
     * @param startOffset the start offset
     * @param endOffset the end offset
     * @param splitter the splitter
     * @param carryInputData the carry input data
     */
    public FormatUtils(int startOffset, int endOffset, FileDataSplitter splitter, boolean carryInputData, GeometryType geometryType)
    {
        this.startOffset = startOffset;
        this.endOffset = endOffset;
        this.splitter = splitter;
        this.carryInputData = carryInputData;
        this.geometryType = geometryType;
        this.allowTopologicallyInvalidGeometries = true;
        this.skipSyntacticallyInvalidGeometries = false;
        // Only the following formats are allowed to use this format mapper because each input has the geometry type definition
        assert geometryType != null || splitter == FileDataSplitter.WKB || splitter == FileDataSplitter.WKT || splitter == FileDataSplitter.GEOJSON;
    }

    /**
     * Instantiates a new format mapper. This is extensively used in SedonaSQL.
     *
     * @param splitter
     * @param carryInputData
     */
    public FormatUtils(FileDataSplitter splitter, boolean carryInputData)
    {
        this(0, -1, splitter, carryInputData, null);
    }

    /**
     * This format mapper is used in SedonaSQL.
     *
     * @param splitter
     * @param carryInputData
     * @param geometryType
     */
    public FormatUtils(FileDataSplitter splitter, boolean carryInputData, GeometryType geometryType)
    {
        this(0, -1, splitter, carryInputData, geometryType);
    }

    public static List<String> readGeoJsonPropertyNames(String geoJson)
    {
        if (geoJson == null) {
            logger.warn("The given GeoJson record is null and cannot be used to find property names");
            return null;
        }
        List<String> propertyList = new ArrayList<>();
        if (geoJson.contains("Feature") || geoJson.contains("feature") || geoJson.contains("FEATURE")) {
            Feature feature = (Feature) GeoJSONFactory.create(geoJson);
            if (!Objects.isNull(feature.getId())) {
                propertyList.add("id");
            }
            Map<String, Object> properties = feature.getProperties();
            if (properties != null) {
                propertyList.addAll(properties.keySet());
            }
        }
        return propertyList.size() > 0 ? propertyList:null;
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
            if (this.splitter != FileDataSplitter.GEOJSON) {
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
            if (feature.getId() != null) {
                nonSpatialData.add(feature.getId().toString());
            }
            if (featurePropertiesproperties != null) {
                for (Object property : featurePropertiesproperties.values()
                ) {
                    if (property == null) {
                        nonSpatialData.add("");
                    }
                    else {
                        nonSpatialData.add(property.toString());
                    }
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

    public List<String> readPropertyNames(String geoString)
    {
        switch (splitter) {
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
        catch (Exception e) {
            logger.error("[Sedona] " + e.getMessage());
        }
        if (geometry == null) {
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
        final int actualEndOffset = this.endOffset >= 0 ? this.endOffset : (this.geometryType == GeometryType.POINT ? startOffset + 1 : columns.length - 1);
        final Coordinate[] coordinates = new Coordinate[(actualEndOffset - startOffset + 1) / 2];
        for (int i = this.startOffset; i <= actualEndOffset; i += 2) {
            coordinates[(i - startOffset) / 2] = new Coordinate(Double.parseDouble(columns[i]), Double.parseDouble(columns[i + 1]));
        }
        if (carryInputData) {
            boolean firstColumnFlag = true;
            otherAttributes = "";
            for (int i = 0; i < this.startOffset; i++) {
                if (firstColumnFlag) {
                    otherAttributes += columns[i];
                    firstColumnFlag = false;
                }
                else { otherAttributes += "\t" + columns[i]; }
            }
            for (int i = actualEndOffset + 1; i < columns.length; i++) {
                if (firstColumnFlag) {
                    otherAttributes += columns[i];
                    firstColumnFlag = false;
                }
                else { otherAttributes += "\t" + columns[i]; }
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
                        throw new IllegalArgumentException("[Sedona][FormatMapper] You must specify GeometryType when you use delimiter rather than WKB, WKT or GeoJSON");
                    }
                    else {
                        geometry = createGeometry(readCoordinates(line), geometryType);
                    }
                }
            }
        }
        catch (Exception e) {
            logger.error("[Sedona] " + e.getMessage());
            if (skipSyntacticallyInvalidGeometries == false) {
                throw e;
            }
        }
        if (geometry == null) {
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
                assert coordinates.length == 2;
                Coordinate[] polyCoordinates = new Coordinate[5];
                polyCoordinates[0] = coordinates[0];
                polyCoordinates[1] = new Coordinate(coordinates[0].x, coordinates[1].y);
                polyCoordinates[2] = coordinates[1];
                polyCoordinates[3] = new Coordinate(coordinates[1].x, coordinates[0].y);
                polyCoordinates[4] = polyCoordinates[0];
                geometry = factory.createPolygon(polyCoordinates);
                break;
            // Read string to point if no geometry type specified but Sedona should never reach here
            default:
                geometry = geometryFactory.createPoint(coordinates[0]);
        }
        if (carryInputData) {
            geometry.setUserData(otherAttributes);
        }
        return geometry;
    }

    protected void addGeometry(Geometry geometry, List<T> result)
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
