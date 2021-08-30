package org.apache.sedona.core.io.avro.utils;

import com.google.common.collect.Lists;
import com.google.common.collect.ImmutableMap;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.sedona.core.enums.GeometryType;
import org.apache.sedona.core.exceptions.SedonaException;
import org.apache.sedona.core.geometryObjects.Circle;
import org.apache.sedona.core.geometryObjects.schema.*;
import org.apache.sedona.core.io.avro.SchemaUtils;
import org.apache.sedona.core.io.avro.constants.AvroConstants;
import org.apache.sedona.core.io.avro.schema.Field;
import org.apache.sedona.core.io.avro.schema.RecordSchema;
import org.locationtech.jts.geom.*;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class AvroUtils {
    /**
     * Joins Namespace & name
     * @param namespace
     * @param name
     * @return Joined String
     */
    public static String getNestedNamespace(String namespace, String... name) {
        
        return String.join(AvroConstants.DOT, namespace, String.join(AvroConstants.DOT, name));
    }
    
    /**
     * Creates Avro Record from a Map of a given Schema
     * @param avroSchema AvroSchema of RecordType
     * @param value
     * @return Avro Record
     * @throws SedonaException
     */
    public static GenericRecord getRecord(Schema avroSchema, Map<String, Object> value) throws SedonaException {
        GenericRecord record = new GenericData.Record(avroSchema);
        for (Schema.Field field : avroSchema.getFields()) {
            record.put(field.name(), getRecord(field.schema(), value.get(field.name())));
        }
        return record;
    }
    
    /**
     * Creates Avro Array from a Collection of a given Schema
     * @param avroSchema Avro Schema of Array Type
     * @param values
     * @return Avro Array
     * @throws SedonaException
     */
    public static GenericArray getArray(Schema avroSchema, Collection<Object> values) throws SedonaException {
        
        List<Object> vals = Lists.newArrayList();
        for (Object val : values) {
            vals.add(getRecord(avroSchema.getElementType(), val));
        }
        return new GenericData.Array<>(avroSchema, vals);
    }
    
    /**
     * Gets an Avro Value from Avro Union Schema
     * @param avroSchema Avro Schema of Union Type
     * @param value
     * @return
     * @throws SedonaException
     */
    public static Object getUnion(Schema avroSchema, Object value) throws SedonaException{
        for(Schema typeSchema:avroSchema.getTypes()){
            try{
                return getRecord(typeSchema,value);
            }catch (SedonaException e){
            }
        }
        throw new SedonaException(String.format("Error while forming Record Given Schema: %s, Given Record: %s",avroSchema.toString(),value==null?null:value.toString()));
    }
    
    public static int getInteger(Object value){
        return Integer.parseInt(value.toString());
    }
    
    public static long getLong(Object value){
        return Long.parseLong(value.toString());
    }
    
    public static double getDouble(Object value){
        return Double.parseDouble(value.toString());
    }
    
    public static float getFloat(Object value){
        return Float.parseFloat(value.toString());
    }
    
    /**
     * Gets a Avro Value from a Java POJO
     * @param avroSchema
     * @param value
     * @return Avro Value
     * @throws SedonaException
     */
    public static Object getRecord(Schema avroSchema, Object value) throws SedonaException {
        try {
            if (value == null) {
                if(Schema.Type.NULL.equals(avroSchema.getType())){
                    return null;
                }
                if(!Schema.Type.UNION.equals(avroSchema.getType())){
                    throw new SedonaException(String.format("Error while forming Record Given Schema: %s, Given Record: %s",avroSchema.toString(),value==null?null:value.toString()));
                }
            }
            if(value instanceof GenericContainer){
                return value;
            }
        
            if (avroSchema == null) {
                throw new SedonaException("Null Schema Provided");
            }
            switch (avroSchema.getType()) {
                case RECORD:
                    return getRecord(avroSchema, (Map<String, Object>) value);
                case ARRAY:
                    return getArray(avroSchema, (Collection<Object>) value);
                case UNION:
                    return getUnion(avroSchema, value);
                case INT:
                    return getInteger(value);
                case LONG:
                    return getLong(value);
                case FLOAT:
                    return getFloat(value);
                case DOUBLE:
                    return getDouble(value);
                default:
                    return value;
            }
        }catch (SedonaException e){
            throw e;
        } catch (Exception e) {
            throw new SedonaException(String.format("Error while forming Record Given Schema: %s, Given Record: %s",avroSchema.toString(),value==null?null:value.toString()),e);
        }
    }
    
    /**
     * Gets Field of a Particular Geometry type
     * @param columnName
     * @param geometryType
     * @return Geometry Field for Output Schema
     * @throws SedonaException
     */
    public static Field getField(String columnName, GeometryType geometryType) throws SedonaException {
        org.apache.sedona.core.io.avro.schema.Schema schema;
        switch (geometryType) {
            case CIRCLE: {
                schema = CircleSchema.getSchema();
                break;
            }
            case POINT: {
                schema = CoordinateSchema.getSchema();
                break;
            }
            case RECTANGLE:
            case POLYGON: {
                schema = PolygonSchema.getSchema();
                break;
            }
            case LINESTRING: {
                schema = CoordinateArraySchema.getSchema();
                break;
            }
            default:
                throw new SedonaException("Geometry Type not Supported");
        }
        return new Field(columnName, schema);
    }
    
    /**
     * Gets Output Schema for Geoemtry
     * @param namespace
     * @param name
     * @param geometryField
     * @param userColumn
     * @return Avro Parquet Schema
     * @throws SedonaException
     */
    public static Schema getGeometryRecordSchema(String namespace,
                                                 String name,
                                                 Field geometryField,
                                                 Field userColumn) throws SedonaException {
        return getGeometryRecordSchema(namespace, name, geometryField, Lists.newArrayList(userColumn));
    }
    
    /**
     * Gets Output Avro Schema from Geometry Field & User Columns
     * @param namespace
     * @param name
     * @param geometryField
     * @param userColumns
     * @return
     * @throws SedonaException
     */
    public static Schema getGeometryRecordSchema(String namespace,
                                                 String name,
                                                 Field geometryField,
                                                 List<Field> userColumns) throws SedonaException {
        List<Field> columns = Lists.newArrayList(geometryField);
        columns.addAll(userColumns);
        RecordSchema schema = new RecordSchema(namespace,name, columns);
        return SchemaUtils.SchemaParser.getSchema(schema.getDataType());
    }
    
    /**
     * Creates Map from Circle Geometry Object
     * @param circle
     * @return Map representing Circle Geometry {c:{x:xValue,y:Value},r:radius}
     */
    public static Map<String, Object> getMapFromCircle(Circle circle) {
        Coordinate center = circle.getCoordinate();
        Double radius = circle.getRadius();
        return ImmutableMap.of(CircleSchema.CENTER, getMapFromCoordinate(center), CircleSchema.RADIUS, radius);
        
    }
    
    /**
     * Creates a Map from Coordinate Object
     * @param coordinate Coordinate Object
     * @return Map representing a Coordinate {x:xValue,y:yValue}
     */
    public static Map<String, Double> getMapFromCoordinate(Coordinate coordinate) {
        return ImmutableMap.of(CoordinateSchema.X_COORDINATE,
                               coordinate.x,
                               CoordinateSchema.Y_COORDINATE,
                               coordinate.y);
    }
    
    /**
     * Gets a List of Maps from Coordinate Array
     * @param coordinates
     * @return List representing a Coordinate Array [{x:xValue,y:yValue}....]
     */
    public static Collection<Map<String, Double>> getCollectionFromCoordinates(Coordinate[] coordinates) {
        return Arrays.stream(coordinates).map(AvroUtils::getMapFromCoordinate).collect(Collectors.toList());
    }
    
    /**
     * Gets a List of Maps from LineString Geometry
     * @param lineString
     * @return List of Maps representing LineString
     */
    public static Collection<Map<String, Double>> getCollectionFromLineString(LineString lineString) {
        return getCollectionFromCoordinates(lineString.getCoordinates());
    }
    
    /**
     * Gets a Map from Point Geometry
     * @param point
     * @return Map represent Point Geometry
     */
    public static Map<String, Double> getMapFromPoint(Point point) {
        return getMapFromCoordinate(point.getCoordinate());
    }
    
    /**
     * Gets a Map from Polygon Geometry
     * @param polygon
     * @return Map representing PolygonGeometry {"ex":[{x:xVal,y:yVal},..],h:[[{x:xVal,y:yVal},..],...]}
     */
    public static Map<String, Object> getMapFromPolygon(Polygon polygon) {
        return ImmutableMap.of(PolygonSchema.EXTERIOR_RING,
                               getCollectionFromCoordinates(polygon.getExteriorRing().getCoordinates()),
                               PolygonSchema.HOLES,
                               IntStream.range(0, polygon.getNumInteriorRing())
                                        .mapToObj(polygon::getInteriorRingN)
                                        .map(linearRing -> getCollectionFromCoordinates(linearRing.getCoordinates()))
                                        .collect(Collectors.toList()));
    }
    
    /**
     * Avro Record from Geometry from POJO representation of Geometry Object & User Data
     * @param schema
     * @param geometryColumnName
     * @param userColumn
     * @param geometryData
     * @param userData
     * @return Avro record
     * @throws SedonaException
     */
    public static GenericRecord getRecordFromGeometry(Schema schema,
                                                      String geometryColumnName,
                                                      String userColumn,
                                                      Object geometryData,
                                                      Object userData) throws SedonaException {
        return (GenericRecord) getRecord(schema,
                                         ImmutableMap.of(geometryColumnName, geometryData, userColumn, userData));
    }
    
    /**
     * Avro Record from Geometry from POJO representation of Geometry Object & User Data
     * @param schema
     * @param geometryColumnName
     * @param geometryData
     * @param userData
     * @return Avro Record
     * @throws SedonaException
     */
    public static GenericRecord getRecordFromGeometry(Schema schema,
                                                      String geometryColumnName,
                                                      Object geometryData,
                                                      Optional<Map<String, Object>> userData) throws SedonaException {
        return (GenericRecord) getRecord(schema,
                                         (Object) new ImmutableMap.Builder<>().putAll(userData.orElse(Collections.EMPTY_MAP))
                                                                              .put(geometryColumnName, geometryData)
                                                                              .build());
    }
    
    /**
     * Creates an Avro Schema for given a geometry Type & User Columns
     * @param geometryColumnName
     * @param userColumns
     * @param namespace
     * @param name
     * @return Geometry Avro Schema
     * @throws SedonaException
     */
    public static Schema getSchema(String geometryColumnName,
                                   List<Field> userColumns,
                                   String namespace,
                                   String name) throws SedonaException {
        Field geometryField = new Field(geometryColumnName, SedonaParquetFileGeometrySchema.getSchema());
        return getGeometryRecordSchema(namespace, name, geometryField, userColumns);
    }
    
    /**
     * Gets Map representation for a given Geometry Object
     */
    public static Map<String,Object> getGeometryData(Geometry geometry, boolean parquetSerialization) throws SedonaException{
        Object geometryData;
        GeometryType type = GeometryType.getGeometryType(geometry.getGeometryType());
        switch (type) {
            case CIRCLE: {
                geometryData = getMapFromCircle((Circle) geometry);
                break;
            }
            case LINESTRING: {
                geometryData = getCollectionFromLineString((LineString) geometry);
                break;
            }
            case POINT: {
                geometryData = getMapFromPoint((Point) geometry);
                break;
            }
            case POLYGON:
            case RECTANGLE: {
                geometryData = getMapFromPolygon((Polygon) geometry);
                break;
            }
            case GEOMETRYCOLLECTION:{
                List<Object> geometryDataList = new ArrayList<>();
                for(int i=0;i<geometry.getNumGeometries();i++){
                    if(geometry.getGeometryN(i) instanceof GeometryCollection && parquetSerialization){
                        throw new SedonaException("Recursive Geometry Collection not Supported for parquet");
                    }
                    geometryDataList.add(getGeometryData(geometry.getGeometryN(i),parquetSerialization));
                }
                geometryData = geometryDataList;
                break;
            }
        
            case MULTIPOINT:{
                List<Object> pointDataList = new ArrayList<>();
                for(int i=0;i<geometry.getNumGeometries();i++){
                    pointDataList.add(getMapFromPoint((Point) geometry.getGeometryN(i)));
                }
                geometryData = pointDataList;
                break;
            }
            case MULTILINESTRING:{
                List<Object> lineStringDataList = new ArrayList<>();
                for(int i=0;i<geometry.getNumGeometries();i++){
                    lineStringDataList.add(getCollectionFromLineString((
                            LineString) geometry.getGeometryN(i)));
                }
                geometryData = lineStringDataList;
                break;
            }
            case MULTIPOLYGON:{
                List<Object> polygonDataList = new ArrayList<>();
                for(int i=0;i<geometry.getNumGeometries();i++){
                    polygonDataList.add(getMapFromPolygon(
                            (Polygon) geometry.getGeometryN(i)));
                }
                geometryData = polygonDataList;
                break;
            }
            default:
                throw new SedonaException("Geometry Type not Supported");
        }
        return ImmutableMap.of(AvroConstants.GEOMETRY_OBJECT,geometryData,
                               AvroConstants.GEOMETRY_SHAPE,type.getName());
    }
    
    /**
     * Avro Record for a given Geometry Object
     * @param geometry
     * @param geometryColumnName
     * @param avroSchema
     * @return Geometry Avro Record representing Geometry Object with Userdata of given Schema
     * @throws SedonaException
     */
    public static GenericRecord getRecord(Geometry geometry,
                                          String geometryColumnName,
                                          Schema avroSchema) throws SedonaException {
        
        return getRecordFromGeometry(avroSchema,
                                     geometryColumnName,
                                     getGeometryData(geometry,true),
                                     Optional.ofNullable((Map<String,Object>)geometry.getUserData()));
        
    }
    
    
}
