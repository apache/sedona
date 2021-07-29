package org.apache.sedona.core.io.avro.utils;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericRecord;
import org.apache.sedona.core.exceptions.SedonaException;
import org.apache.sedona.core.geometryObjects.Circle;
import org.apache.sedona.core.geometryObjects.schema.CircleSchema;
import org.apache.sedona.core.geometryObjects.schema.CoordinateSchema;
import org.apache.sedona.core.geometryObjects.schema.PolygonSchema;
import org.apache.sedona.core.io.avro.SchemaUtils;
import org.apache.sedona.core.io.avro.constants.AvroConstants;
import org.apache.sedona.core.io.avro.schema.*;
import org.junit.Test;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.util.Assert;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class AvroUtilsTest extends BaseSchemaTest {
    GeometryFactory geometryFactory = new GeometryFactory();
    @Test
    public void testGetNestedNameSpace(){
        Assert.equals(AvroUtils.getNestedNamespace("org.namespace","name1","name2"),"org.namespace.name1.name2");
    }
    
    @Test
    public void testValidGetRecord() throws SedonaException {
        synchronized (SchemaUtils.SchemaParser.class) {
            Schema schema = new RecordSchema(TEST_NAMESPACE, TEST_NAME, Lists.newArrayList(new Field("a", new RecordSchema(TEST_NAMESPACE,"a",
                                                                                                                           Lists.newArrayList(
                                                                                                                                   new Field("a1",new SimpleSchema(
                                                                                                                                           AvroConstants.PrimitiveDataType.INT)),
                                                                                                                                   new Field("a2", new SimpleSchema(
                                                                                                                                           AvroConstants.PrimitiveDataType.DOUBLE))))),
                                                                                           new Field("b", new ArraySchema(new RecordSchema(TEST_NAMESPACE,"b",
                                                                                                                                           Lists.newArrayList(
                                                                                                                                                   new Field("b1",new SimpleSchema(AvroConstants.PrimitiveDataType.DOUBLE)),
                                                                                                                                                   new Field("b2",new SimpleSchema(AvroConstants.PrimitiveDataType.DOUBLE))
                                                                                                                                                             )
                                                                                           )))
                                                                                          )
            );
            org.apache.avro.Schema avroSchema = SchemaUtils.SchemaParser.getSchema(schema.getDataType().toString());
            Map<String,Object> data = ImmutableMap.of("a", ImmutableMap.of("a1", 2, "a2", 3.14),
                                                      "b", Lists.newArrayList(
                                                              ImmutableMap.of("b1", 3.14, "b2", 3.15),
                                                              ImmutableMap.of("b1", 3.16, "b2", 3.17)));
            GenericRecord genericRecord = AvroUtils.getRecord(avroSchema, data);
            Assert.equals(genericRecord.getSchema(),avroSchema);
            Assert.equals(((GenericRecord)genericRecord.get("a")).get("a1"),((Map<String,Object>)data.get("a")).get("a1"));
            Assert.equals(((GenericRecord)genericRecord.get("a")).get("a2"),((Map<String,Object>)data.get("a")).get("a2"));
        
            Assert.equals(((GenericRecord)((GenericArray)genericRecord.get("b")).get(0)).get("b1"), ((List<Map<String,Object>>)data.get("b")).get(0).get("b1"));
            Assert.equals(((GenericRecord)((GenericArray)genericRecord.get("b")).get(0)).get("b2"), ((List<Map<String,Object>>)data.get("b")).get(0).get("b2"));
        
            Assert.equals(((GenericRecord)((GenericArray)genericRecord.get("b")).get(1)).get("b1"), ((List<Map<String,Object>>)data.get("b")).get(1).get("b1"));
            Assert.equals(((GenericRecord)((GenericArray)genericRecord.get("b")).get(1)).get("b2"), ((List<Map<String,Object>>)data.get("b")).get(1).get("b2"));
        }
    }
    
    @Test(expected = SedonaException.class)
    public void testInvalidGetRecord() throws SedonaException {
        Schema schema = new RecordSchema(TEST_NAMESPACE, TEST_NAME, Lists.newArrayList(new Field("a", new RecordSchema(TEST_NAMESPACE,"a",
                                                                                                                       Lists.newArrayList(
                                                                                                                               new Field("a1",new SimpleSchema(
                                                                                                                                       AvroConstants.PrimitiveDataType.INT)),
                                                                                                                               new Field("a2", new SimpleSchema(
                                                                                                                                       AvroConstants.PrimitiveDataType.DOUBLE))))),
                                                                                       new Field("b", new ArraySchema(new RecordSchema(TEST_NAMESPACE,"b",
                                                                                                                                       Lists.newArrayList(
                                                                                                                                               new Field("b1",new SimpleSchema(AvroConstants.PrimitiveDataType.DOUBLE)),
                                                                                                                                               new Field("b2",new SimpleSchema(AvroConstants.PrimitiveDataType.DOUBLE))
                                                                                                                                                         )
                                                                                       ))),
                                                                                       new Field("c", new UnionSchema(
                                                                                               new SimpleSchema(AvroConstants.PrimitiveDataType.INT),
                                                                                               new SimpleSchema(AvroConstants.PrimitiveDataType.NULL)))
                                                                                      )
        );
        org.apache.avro.Schema avroSchema = SchemaUtils.SchemaParser.getSchema(schema.getDataType().toString());
        Map<String,Object> data = ImmutableMap.of("a", ImmutableMap.of("a1", 2, "a2", 3.14),
                                                  "b", Lists.newArrayList(
                                                          ImmutableMap.of("b1", 3.14, "b2", 3.15),
                                                          ImmutableMap.of("b1", 3.16, "b2", 3.17)));
        GenericRecord genericRecord = AvroUtils.getRecord(avroSchema, data);
        Assert.isTrue(genericRecord.get("c")==null);
        data = ImmutableMap.of("b",data.get("b"));
        genericRecord = AvroUtils.getRecord(avroSchema, data);
    
    }
    
    @Test
    public void testGetMapFromPoint(){
        Point p = geometryFactory.createPoint(new Coordinate(1.0,2.1));
        Map<String,Double> map =  AvroUtils.getMapFromPoint(p);
        Assert.equals(map.get(CoordinateSchema.X_COORDINATE),1.0);
        Assert.equals(map.get(CoordinateSchema.Y_COORDINATE),2.1);
    }
    
    @Test
    public void testGetMapFromCircle(){
        Circle circle = new Circle(geometryFactory.createPoint(new Coordinate(1.0,2.1)),1.0);
        Map<String,Object> map =  AvroUtils.getMapFromCircle(circle);
        Assert.equals(((Map<String,Double>)map.get(CircleSchema.CENTER)).get(CoordinateSchema.X_COORDINATE), 1.0);
        Assert.equals(((Map<String,Double>)map.get(CircleSchema.CENTER)).get(CoordinateSchema.Y_COORDINATE),2.1);
        Assert.equals(map.get(CircleSchema.RADIUS),1.0);
    }
    
    @Test
    public void testGetMapFromCoordinate(){
        Coordinate coordinate = new Coordinate(1.0,2.1);
        Map<String,Double> map =  AvroUtils.getMapFromCoordinate(coordinate);
        Assert.equals(map.get(CoordinateSchema.X_COORDINATE),1.0);
        Assert.equals(map.get(CoordinateSchema.Y_COORDINATE),2.1);
    }
    
    @Test
    public void testGetCollectionFromCoordinates(){
        Coordinate[] coordinates = new Coordinate[]{
                new Coordinate(1.0,2.1),
                new Coordinate(1.1,2.2),
                new Coordinate(1.3,2.3)
        };
        int i = 0;
        for(Map<String,Double> cMap:AvroUtils.getCollectionFromCoordinates(coordinates)){
            Assert.equals(cMap,AvroUtils.getMapFromCoordinate(coordinates[i]));
            i++;
        }
    }
    
    @Test
    public void testGetCollectionFromLineString(){
        LineString coordinates = geometryFactory.createLineString(new Coordinate[]{
                new Coordinate(1.0,2.1),
                new Coordinate(1.1,2.2),
                new Coordinate(1.3,2.3)
        });
        int i = 0;
        for(Map<String,Double> cMap:AvroUtils.getCollectionFromLineString(coordinates)){
            Assert.equals(cMap,AvroUtils.getMapFromCoordinate(coordinates.getCoordinateN(i)));
            i++;
        }
    }
    
    @Test
    public void testGetMapFromPolygon(){
        Polygon polygon = geometryFactory.createPolygon(geometryFactory.createLinearRing(new Coordinate[]{
                new Coordinate(0,0),
                new Coordinate(10,0),
                new Coordinate(10,10),
                new Coordinate(0,10),
                new Coordinate(0,0)
        }),new LinearRing[]{geometryFactory.createLinearRing(new Coordinate[]{
                new Coordinate(0,5),
                new Coordinate(5,5),
                new Coordinate(5, 0),
                new Coordinate(0,5)
        }), geometryFactory.createLinearRing(new Coordinate[]{
                new Coordinate(0,0),
                new Coordinate(5, 0),
                new Coordinate(0,5),
                new Coordinate(0,0)
        })});
        Map<String,Object> map = AvroUtils.getMapFromPolygon(polygon);
        Assert.equals(map.get(PolygonSchema.EXTERIOR_RING), Arrays.asList(ImmutableMap.of(CoordinateSchema.X_COORDINATE,0.0,CoordinateSchema.Y_COORDINATE,0.0),
                                                                          ImmutableMap.of(CoordinateSchema.X_COORDINATE,10.0,CoordinateSchema.Y_COORDINATE,0.0),
                                                                          ImmutableMap.of(CoordinateSchema.X_COORDINATE,10.0,CoordinateSchema.Y_COORDINATE,10.0),
                                                                          ImmutableMap.of(CoordinateSchema.X_COORDINATE,0.0,CoordinateSchema.Y_COORDINATE,10.0),
                                                                          ImmutableMap.of(CoordinateSchema.X_COORDINATE,0.0,CoordinateSchema.Y_COORDINATE,0.0)));
        Assert.equals(((List)map.get(PolygonSchema.HOLES)).get(0), Arrays.asList(ImmutableMap.of(CoordinateSchema.X_COORDINATE,0.0,CoordinateSchema.Y_COORDINATE,5.0),
                                                                          ImmutableMap.of(CoordinateSchema.X_COORDINATE,5.0,CoordinateSchema.Y_COORDINATE,5.0),
                                                                          ImmutableMap.of(CoordinateSchema.X_COORDINATE,5.0,CoordinateSchema.Y_COORDINATE,0.0),
                                                                          ImmutableMap.of(CoordinateSchema.X_COORDINATE,0.0,CoordinateSchema.Y_COORDINATE,5.0)));
        Assert.equals(((List)map.get(PolygonSchema.HOLES)).get(1), Arrays.asList(ImmutableMap.of(CoordinateSchema.X_COORDINATE,0.0,CoordinateSchema.Y_COORDINATE,0.0),
                                                                                 ImmutableMap.of(CoordinateSchema.X_COORDINATE,5.0,CoordinateSchema.Y_COORDINATE,0.0),
                                                                                 ImmutableMap.of(CoordinateSchema.X_COORDINATE,0.0,CoordinateSchema.Y_COORDINATE,5.0),
                                                                                 ImmutableMap.of(CoordinateSchema.X_COORDINATE,0.0,CoordinateSchema.Y_COORDINATE,0.0)));
    
    }
}
