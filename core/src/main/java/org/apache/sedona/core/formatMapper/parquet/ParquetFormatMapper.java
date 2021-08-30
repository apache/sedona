package org.apache.sedona.core.formatMapper.parquet;

import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericRecord;
import org.apache.log4j.Logger;
import org.apache.sedona.core.enums.GeometryType;
import org.apache.sedona.core.exceptions.SedonaException;
import org.apache.sedona.core.exceptions.SedonaRuntimeException;
import org.apache.sedona.core.formatMapper.FormatMapper;
import org.apache.sedona.core.geometryObjects.Circle;
import org.apache.sedona.core.geometryObjects.schema.*;
import org.apache.sedona.core.io.avro.constants.AvroConstants;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.locationtech.jts.geom.*;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

/**
 * ParquetFormatMapper class
 * Responsible for converting Avro-Parquet Records into Geometry Objects
 * @param <T> Geometry Class Type
 */
public class ParquetFormatMapper<T extends Geometry> implements Serializable, FlatMapFunction<Iterator<GenericRecord>, T> {
    final static Logger logger = Logger.getLogger(FormatMapper.class);
    protected GeometryFactory factory = new GeometryFactory();
    private final String geometryColumn;
    private final List<String> userColumns;
    private final GeometryType defaultGeometryType;
    
    /**
     * @param geometryColumn
     * @param userColumns
     * @param defaultGeometryType
     */
    public ParquetFormatMapper(String geometryColumn, List<String> userColumns, GeometryType defaultGeometryType) {
        this.geometryColumn = geometryColumn;
        this.userColumns = userColumns;
        this.defaultGeometryType = defaultGeometryType;
    }
    
    /**
     * @param geometryColumn
     * @param userColumns
     */
    public ParquetFormatMapper(String geometryColumn, List<String> userColumns) {
        this(geometryColumn,userColumns,null);
    }
    
    /**
     * Gets Coordinate from Avro Record
     * @param record Avro Record
     * @return Coordinate
     */
    private static Coordinate getCoordinate(GenericRecord record) {
        Double x = (Double) record.get(CoordinateSchema.X_COORDINATE);
        Double y = (Double) record.get(CoordinateSchema.Y_COORDINATE);
        return new Coordinate(x, y);
    }
    
    /**
     * Gets Coordinate from Avro based Record
     * @param val
     * @return Coordinate
     */
    private static Coordinate getCoordinate(GenericContainer val) {
        if (val instanceof GenericRecord) {
            return getCoordinate((GenericRecord) val);
        }
        return getCoordinate((GenericArray) val);
    }
    /**
     * Gets Coordinate from Avro based Array based on the first index
     * @param array
     * @return Coordinate
     */
    private static Coordinate getCoordinate(GenericArray array) {
        return getCoordinate((GenericRecord) array.get(0));
    }
    
    /**
     * Gets Coordinate Array from Avro Record Array
     * @param array
     * @return Coordinate Array
     */
    private static Coordinate[] getCoordinates(GenericArray array) {
        Coordinate[] coordinates = new Coordinate[array.size()];
        for(int i=0;i<array.size();i++){
            coordinates[i] = getCoordinate((GenericRecord) array.get(i));
        }
        return coordinates;
    }
    
    /**
     * Gets Point Geometry from Coordinate
     * @param coordinate
     * @return Point
     */
    private Point getPoint(Coordinate coordinate) {
        return factory.createPoint(coordinate);
    }
    
    /**
     * Gets Circle Geometry Object from Avro Record
     * @param record
     * @return Circle
     */
    private Circle getCircle(GenericRecord record) {
        Coordinate center = getCoordinate((GenericRecord) record.get(CircleSchema.CENTER));
        Double radius = (Double) record.get(CircleSchema.RADIUS);
        return new Circle(getPoint(center), radius);
    }
    /**
     * Gets Polygon Geometry Object from Avro Record
     * @param record
     * @return Polygon
     */
    private Polygon getPolygon(GenericRecord record) {
        LinearRing exteriorRing =
                factory.createLinearRing(getCoordinates((GenericArray) record.get(PolygonSchema.EXTERIOR_RING)));
        Optional<GenericArray> holes =  Optional.ofNullable((GenericArray) record.get(PolygonSchema.HOLES));
        List<LinearRing> interiorRings = holes.map(arr->IntStream.range(0, arr.size())
                                                            .mapToObj(arr::get)
                                                            .map(hole -> getCoordinates((GenericArray) (hole)))
                                                            .map(c -> factory.createLinearRing(c))
                                                            .collect(Collectors.toList())).orElse(Collections.EMPTY_LIST);
        
        return factory.createPolygon(exteriorRing, (LinearRing[]) interiorRings.toArray(new LinearRing[interiorRings.size()]));
    }
    
    /**
     * Gets Geometry Object of Geometry Type from Coordinate Array
     * @param coordinates CoordinateArray
     * @param type geometryType
     * @return Geometry Object
     */
    private Geometry getGeometry(Coordinate[] coordinates, GeometryType type) {
        Geometry geometry = null;
        switch (type) {
            case POINT:
                geometry = factory.createPoint(coordinates[0]);
                break;
            case POLYGON:
                geometry = factory.createPolygon(coordinates);
                break;
            case LINESTRING:
                geometry = factory.createLineString(coordinates);
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
                geometry = factory.createPoint(coordinates[0]);
        }
        return geometry;
    }
    
    /**
     * Gets GeometrCollection from Array of Avro Records
     * @param array
     * @return GeometryCollection POJO
     */
    private GeometryCollection getGeometryCollection(GenericArray array){
        Geometry[] geometries = new Geometry[array.size()];
        for(int i=0;i<geometries.length;i++){
            geometries[i] = getGeometry((GenericRecord) array.get(i));
        }
        return factory.createGeometryCollection(geometries);
    }
    
    /**
     * Gets MultiLineString from Array of Avro LineString Records
     * @param array
     * @return MultiLineString POJO
     */
    private MultiLineString getMultiLineString(GenericArray array){
        LineString[] lineStrings = new LineString[array.size()];
        for(int i=0;i<lineStrings.length;i++){
            lineStrings[i] = (LineString) getGeometry(getCoordinates((GenericArray) array.get(i)),
                                                      GeometryType.LINESTRING);
        }
        return factory.createMultiLineString(lineStrings);
    }
    
    /**
     * Gets MultiPolygon from Array of Avro Polygon Records
     * @param array
     * @return MultiPolygon
     */
    private MultiPolygon getMultiPolygon(GenericArray array){
        Polygon[] polygons = new Polygon[array.size()];
        for(int i=0;i<polygons.length;i++){
            polygons[i] = getPolygon((GenericRecord) array.get(i));
        }
        return factory.createMultiPolygon(polygons);
    }
    
    /**
     * Gets MultiPoint from Array of Avro Point Records
     * @param array
     * @return MultiPoint POJO
     */
    private MultiPoint getMultiPoint(GenericArray array){
        Point[] points = new Point[array.size()];
        for(int i=0;i<points.length;i++){
            points[i] = getPoint(getCoordinate((GenericContainer) array.get(i)));
        }
        return factory.createMultiPoint(points);
    }
    
    
    /**
     * Gets Geometry Object of Geometry Type from Avro Record
     * @param geometryColumn
     * @return Geometry
     */
    private Geometry getGeometry(Object geometryColumn) {
        if(geometryColumn instanceof GenericArray){
            try{
                if(this.defaultGeometryType!=null){
                    return getGeometry(getCoordinates((GenericArray) geometryColumn),defaultGeometryType);
                }
            }catch (Exception e){
            
            }
            return getGeometryCollection((GenericArray) geometryColumn);
        }
        GenericContainer geometryObject =
                (GenericContainer) ((GenericRecord)geometryColumn).get(AvroConstants.GEOMETRY_OBJECT);
        GeometryType geometryType = GeometryType.getGeometryType(((GenericRecord)geometryColumn)
                                                                         .get(AvroConstants.GEOMETRY_SHAPE)
                                                                         .toString());
        switch (geometryType) {
            case CIRCLE:
                return getCircle((GenericRecord) geometryObject);
            case POINT:
                return getPoint(getCoordinate(geometryObject));
            case LINESTRING:
                return getGeometry(getCoordinates((GenericArray) geometryObject),GeometryType.LINESTRING);
            case POLYGON:
            case RECTANGLE: {
                return getPolygon((GenericRecord) geometryObject);
            }
            case GEOMETRYCOLLECTION:{
                return getGeometryCollection((GenericArray) geometryObject);
            }
            case MULTIPOINT:{
                return getMultiPoint((GenericArray) geometryObject);
            }
            case MULTILINESTRING:{
                return getMultiLineString((GenericArray) geometryObject);
            }
            case MULTIPOLYGON:{
                return getMultiPolygon((GenericArray) geometryObject);
            }
        }
        throw new SedonaRuntimeException("Invalid Avro Geometry Record :"+geometryColumn.toString());
    }
    
    /**
     * Sets User Data in a geometry Object based on the Parquet Read Schema Index(Geometry Column+<List of User Columns to be read>)
     * @param geometry Geomery Object
     * @param genericRecord Avro Record
     */
    private void setUserData(Geometry geometry, GenericRecord genericRecord) {
        geometry.setUserData(IntStream.range(0, userColumns.size())
                                      .mapToObj(i -> i)
                                      .collect(Collectors.toMap(userColumns::get,
                                                                i -> genericRecord.get(i + 1))));
    }
    
    /**
     * Maps Avro Records to their Geometry Object
     * @param genericRecordIterator
     * @return
     * @throws Exception
     */
    @Override
    public Iterator<T> call(Iterator<GenericRecord> genericRecordIterator) throws Exception {
        Iterable<GenericRecord> recordIterable = () -> genericRecordIterator;
        return StreamSupport.stream(recordIterable.spliterator(), false).map(record -> {
            T geometry = (T) getGeometry(record.get(this.geometryColumn));
            setUserData(geometry, record);
            return geometry;
        }).iterator();
    }
}
