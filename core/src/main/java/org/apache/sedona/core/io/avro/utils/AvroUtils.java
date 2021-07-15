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
import org.apache.sedona.core.geometryObjects.schema.CircleSchema;
import org.apache.sedona.core.geometryObjects.schema.CoordinateArraySchema;
import org.apache.sedona.core.geometryObjects.schema.CoordinateSchema;
import org.apache.sedona.core.geometryObjects.schema.PolygonSchema;
import org.apache.sedona.core.io.avro.SchemaUtils;
import org.apache.sedona.core.io.avro.constants.AvroConstants;
import org.apache.sedona.core.io.avro.schema.Field;
import org.apache.sedona.core.io.avro.schema.SimpleSchema;
import org.apache.sedona.core.io.avro.schema.RecordSchema;
import org.locationtech.jts.geom.*;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class AvroUtils {
    public static String getNestedNamespace(String namespace, String... name) {
        
        return String.join(AvroConstants.DOT, namespace, String.join(AvroConstants.DOT, name));
    }
    
    
    public static GenericRecord getRecord(Schema avroSchema, Map<String, Object> value) throws SedonaException {
        GenericRecord record = new GenericData.Record(avroSchema);
        for (Schema.Field field : avroSchema.getFields()) {
            record.put(field.name(), getRecord(field.schema(), value.get(field.name())));
        }
        return record;
    }
    
    public static GenericArray getArray(Schema avroSchema, Collection<Object> values) throws SedonaException {
        
        List<Object> vals = Lists.newArrayList();
        for (Object val : values) {
            vals.add(getRecord(avroSchema.getElementType(), val));
        }
        return new GenericData.Array<>(avroSchema, vals);
    }
    
    public static Object getRecord(Schema avroSchema, Object value) throws SedonaException {
        if (value == null) {
            return null;
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
            default:
                return value;
        }
    }
    
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
    
    public static Schema getGeometryRecordSchema(String namespace,
                                                 String name,
                                                 Field geometryField,
                                                 Field userColumn) throws SedonaException {
        return getGeometryRecordSchema(namespace, name, geometryField, Lists.newArrayList(userColumn));
    }
    
    public static Schema getGeometryRecordSchema(String namespace,
                                                 String name,
                                                 Field geometryField,
                                                 List<Field> userColumns) throws SedonaException {
        List<Field> columns = Lists.newArrayList(geometryField);
        columns.addAll(userColumns);
        RecordSchema schema = new RecordSchema(namespace,name, columns);
        return SchemaUtils.SchemaParser.getSchema(schema.getDataType());
    }
    
    public static Map<String, Object> getMapRecordFromCircle(Circle circle) {
        Coordinate center = circle.getCoordinate();
        Double radius = circle.getRadius();
        return ImmutableMap.of(CircleSchema.CENTER, getMapFromCoordinate(center), CircleSchema.RADIUS, radius);
        
    }
    
    public static Map<String, Double> getMapFromCoordinate(Coordinate coordinate) {
        return ImmutableMap.of(CoordinateSchema.X_COORDINATE,
                               coordinate.x,
                               CoordinateSchema.Y_COORDINATE,
                               coordinate.y);
    }
    
    public static Collection<Map<String, Double>> getCollectionFromCoordinates(Coordinate[] coordinates) {
        return Arrays.stream(coordinates).map(AvroUtils::getMapFromCoordinate).collect(Collectors.toList());
    }
    
    public static Collection<Map<String, Double>> getCollectionFromLineString(LineString lineString) {
        return getCollectionFromCoordinates(lineString.getCoordinates());
    }
    
    public static Map<String, Double> getMapFromPoint(Point point) {
        return getMapFromCoordinate(point.getCoordinate());
    }
    
    public static Map<String, Object> getMapFromPolygon(Polygon polygon) {
        return ImmutableMap.of(PolygonSchema.EXTERIOR_RING,
                               getCollectionFromCoordinates(polygon.getExteriorRing().getCoordinates()),
                               PolygonSchema.HOLES,
                               IntStream.range(0, polygon.getNumInteriorRing())
                                        .mapToObj(polygon::getInteriorRingN)
                                        .map(linearRing -> getCollectionFromCoordinates(linearRing.getCoordinates()))
                                        .collect(Collectors.toList()));
    }
    
    public static GenericRecord getRecordFromGeometry(Schema schema,
                                                      String geometryColumnName,
                                                      String userColumn,
                                                      Object geometryData,
                                                      Object userData) throws SedonaException {
        return (GenericRecord) getRecord(schema,
                                         ImmutableMap.of(geometryColumnName, geometryData, userColumn, userData));
    }
    
    public static GenericRecord getRecordFromGeometry(Schema schema,
                                                      String geometryColumnName,
                                                      Object geometryData,
                                                      Map<String, Object> userData) throws SedonaException {
        return (GenericRecord) getRecord(schema,
                                         (Object) new ImmutableMap.Builder<>().putAll(userData)
                                                                              .put(geometryColumnName, geometryData)
                                                                              .build());
    }
    
    public static Schema getSchema(GeometryType geometryType,
                                   String geometryColumnName,
                                   List<Field> userColumns,
                                   String namespace,
                                   String name) throws SedonaException {
        Field geometryField = getField(geometryColumnName, geometryType);
        return getGeometryRecordSchema(namespace, name, geometryField, userColumns);
    }
    
    public static GenericRecord getRecord(Geometry geometry,
                                          GeometryType type,
                                          String geometryColumnName,
                                          Schema avroSchema) throws SedonaException {
        Object geometryData;
        switch (type) {
            case CIRCLE: {
                geometryData = getMapRecordFromCircle((Circle) geometry);
                break;
            }
            case LINESTRING: {
                geometryData = getCollectionFromLineString((LineString) geometry);
                break;
            }
            case POINT: {
                geometryData = getMapFromCoordinate(((Point) geometry).getCoordinate());
                break;
            }
            case POLYGON:
            case RECTANGLE: {
                geometryData = getMapFromPolygon((Polygon) geometry);
                break;
            }
            default:
                throw new SedonaException("Geometry Type not Supported");
        }
        return getRecordFromGeometry(avroSchema,
                                     geometryColumnName,
                                     geometryData,
                                     (Map<String, Object>) geometry.getUserData());
        
    }
    
    
}
