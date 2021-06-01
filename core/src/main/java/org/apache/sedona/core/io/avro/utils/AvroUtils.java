package org.apache.sedona.core.io.avro.utils;

import com.google.common.collect.Lists;
import com.google.common.collect.ImmutableMap;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.sedona.core.enums.GeometryType;
import org.apache.sedona.core.exceptions.SedonaException;
import org.apache.sedona.core.geometryObjects.Circle;
import org.apache.sedona.core.geometryObjects.schema.CircleSchema;
import org.apache.sedona.core.geometryObjects.schema.CoordinateArraySchema;
import org.apache.sedona.core.geometryObjects.schema.CoordinateSchema;
import org.apache.sedona.core.geometryObjects.schema.PolygonSchema;
import org.apache.sedona.core.io.avro.constants.AvroConstants;
import org.apache.sedona.core.io.avro.schema.NestedSchema;
import org.apache.sedona.core.io.avro.schema.PrimitiveSchema;
import org.apache.sedona.core.io.avro.schema.RecordSchema;
import org.locationtech.jts.geom.*;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class AvroUtils {
    public static GenericRecord getRecord(Schema avroSchema,
                                          Map<String, Object> value) throws SedonaException {
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
    
    public static org.apache.sedona.core.io.avro.schema.Schema getCircleSchema(String namespace,
                                                                               String columnName) {
        return new NestedSchema(columnName, new CircleSchema(namespace, GeometryType.CIRCLE.getName()));
        
    }
    
    public static org.apache.sedona.core.io.avro.schema.Schema getPolygonSchema(String namespace,
                                                                                String columnName) {
        return new NestedSchema(columnName, new PolygonSchema(namespace, GeometryType.POLYGON.getName()));
    }
    
    public static org.apache.sedona.core.io.avro.schema.Schema getPointSchema(String namespace,
                                                                              String columnName) {
        return new NestedSchema(columnName, new CoordinateSchema(namespace, GeometryType.POINT.getName()));
    }
    
    public static org.apache.sedona.core.io.avro.schema.Schema getCoordinateArraySchema(String columnName,
                                                                                        String namespace) {
        return new CoordinateArraySchema(columnName, namespace);
    }
    
    public static Schema getGeometryRecordSchema(String namespace,
                                                 String name,
                                                 org.apache.sedona.core.io.avro.schema.Schema geometrySchema,
                                                 String userColumn) throws SedonaException {
        return getGeometryRecordSchema(namespace, name, geometrySchema, Lists.newArrayList(userColumn));
    }
    
    public static Schema getGeometryRecordSchema(String namespace,
                                                 String name,
                                                 org.apache.sedona.core.io.avro.schema.Schema geometrySchema,
                                                 List<String> userColumns) throws SedonaException {
        List<org.apache.sedona.core.io.avro.schema.Schema> columns = Lists.newArrayList(geometrySchema);
        columns.addAll(userColumns.stream()
                               .map(col -> new PrimitiveSchema(col, AvroConstants.PrimitiveDataType.STRING))
                               .collect(Collectors.toList()));
        return new RecordSchema(namespace, name, columns).getAvroSchema();
    }
    
    public static Map<String, Object> getMapRecordFromCircle(Circle circle) {
        Coordinate center = circle.getCoordinate();
        Double radius = circle.getRadius();
        return ImmutableMap.of(CircleSchema.CENTER, getMapFromCoordinate(center), CircleSchema.RADIUS,
                               radius);
        
    }
    
    public static Map<String, Double> getMapFromCoordinate(Coordinate coordinate) {
        return ImmutableMap.of(CoordinateSchema.X_COORDINATE, coordinate.x, CoordinateSchema.Y_COORDINATE,
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
                               PolygonSchema.HOLES, IntStream.range(0, polygon.getNumInteriorRing())
                                       .mapToObj(polygon::getInteriorRingN)
                                       .map(linearRing -> ImmutableMap.of(PolygonSchema.HOLE,
                                                                          getCollectionFromCoordinates(
                                                                                  linearRing.getCoordinates())))
                                       .collect(Collectors.toList()));
    }
    
    public static GenericRecord getRecordFromGeometry(Schema schema,
                                                      String geometryColumnName,
                                                      String userColumn,
                                                      Object geometryData,
                                                      Object userData) throws SedonaException {
        return (GenericRecord) getRecord(schema, ImmutableMap.of(geometryColumnName, geometryData, userColumn,
                                                                 userData));
    }
    
    public static GenericRecord getRecordFromGeometry(Schema schema,
                                                      String geometryColumnName,
                                                      Object geometryData,
                                                      Map<String, Object> userData) throws SedonaException {
        return (GenericRecord) getRecord(schema, (Object) new ImmutableMap.Builder<>().putAll(userData)
                .put(geometryColumnName, geometryData)
                .build());
    }
    
    public static Schema getSchema(GeometryType geometryType,
                                   String geometryColumnName,
                                   List<String> userColumns,
                                   String name) throws SedonaException {
        String geometryNamespace = String.join(AvroConstants.DOT, AvroConstants.SEDONA_NAMESPACE, name);
        switch (geometryType){
            case CIRCLE: {
                org.apache.sedona.core.io.avro.schema.Schema geometrySchema =
                        getCircleSchema(geometryNamespace, geometryColumnName);
                return getGeometryRecordSchema(AvroConstants.SEDONA_NAMESPACE, name, geometrySchema,
                                                     userColumns);
            }
            case LINESTRING: {
                org.apache.sedona.core.io.avro.schema.Schema geometrySchema =
                        getCoordinateArraySchema(geometryNamespace, geometryColumnName);
                return getGeometryRecordSchema(AvroConstants.SEDONA_NAMESPACE, name, geometrySchema,
                                                     userColumns);
            }
            case POINT: {
                org.apache.sedona.core.io.avro.schema.Schema geometrySchema =
                        getPointSchema(geometryNamespace, geometryColumnName);
                return getGeometryRecordSchema(AvroConstants.SEDONA_NAMESPACE, name, geometrySchema,
                                                     userColumns);
            }
            case POLYGON:
            case RECTANGLE: {
                org.apache.sedona.core.io.avro.schema.Schema geometrySchema =
                        getPolygonSchema(geometryNamespace, geometryColumnName);
                return getGeometryRecordSchema(AvroConstants.SEDONA_NAMESPACE, name, geometrySchema,
                                                     userColumns);
            }
        }
        throw new SedonaException("Geometry Type not Supported");
    }
    
    public static GenericRecord getRecord(Geometry geometry,
                                          GeometryType type,
                                          String geometryColumnName,
                                          Schema avroSchema
                                          ) throws SedonaException {
        
        switch (type) {
            case CIRCLE: {
                Object geometryData = getMapRecordFromCircle((Circle) geometry);
                return getRecordFromGeometry(avroSchema, geometryColumnName, geometryData,
                                             (Map<String, Object>) geometry.getUserData());
            }
            case LINESTRING: {
                Object geometryData = getCollectionFromLineString((LineString) geometry);
                return getRecordFromGeometry(avroSchema, geometryColumnName, geometryData,
                                             (Map<String, Object>) geometry.getUserData());
            }
            case POINT: {
                Object geometryData = getMapFromCoordinate(((Point) geometry).getCoordinate());
                return getRecordFromGeometry(avroSchema, geometryColumnName, geometryData,
                                             (Map<String, Object>) geometry.getUserData());
            }
            case POLYGON:
            case RECTANGLE: {
                Object geometryData = getMapFromPolygon((Polygon) geometry);
                return getRecordFromGeometry(avroSchema, geometryColumnName, geometryData,
                                             (Map<String, Object>) geometry.getUserData());
            }
            
            
        }
        throw new SedonaException("Geometry Type not Supported");
    }
    
    
}
