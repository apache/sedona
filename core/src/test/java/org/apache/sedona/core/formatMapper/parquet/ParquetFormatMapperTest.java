package org.apache.sedona.core.formatMapper.parquet;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
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
import org.apache.sedona.core.io.avro.schema.*;
import org.apache.sedona.core.io.avro.utils.AvroUtils;
import org.junit.BeforeClass;
import org.junit.Test;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.util.Assert;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.StreamSupport;

public class ParquetFormatMapperTest extends BaseSchemaTest {
    
    
    private static GenericRecord arrayRecord;
    private static GenericRecord rectArrayRecord;
    private static GenericRecord circleRecord;
    private static GenericRecord polygonRecordWithHoles;
    private static GenericRecord polygonRecordWithoutHoles;
    @BeforeClass
    public static void init() throws SedonaException {
        Schema arrSchema = new RecordSchema(TEST_NAMESPACE, "array",
                                            Lists.newArrayList(new Field("arr", CoordinateArraySchema.getSchema()),
                                                               new Field("i",new SimpleSchema(AvroConstants.PrimitiveDataType.INT)),
                                                               new Field("j",new SimpleSchema(AvroConstants.PrimitiveDataType.STRING))));
        arrayRecord = AvroUtils.getRecord(SchemaUtils.SchemaParser.getSchema(arrSchema.getDataType().toString()), ImmutableMap.of("arr",Lists.newArrayList(
                ImmutableMap.of(CoordinateSchema.X_COORDINATE,0, CoordinateSchema.Y_COORDINATE, 0),
                ImmutableMap.of(CoordinateSchema.X_COORDINATE,10, CoordinateSchema.Y_COORDINATE, 0),
                ImmutableMap.of(CoordinateSchema.X_COORDINATE,10, CoordinateSchema.Y_COORDINATE, 10),
                ImmutableMap.of(CoordinateSchema.X_COORDINATE,0, CoordinateSchema.Y_COORDINATE, 10),
                ImmutableMap.of(CoordinateSchema.X_COORDINATE,0, CoordinateSchema.Y_COORDINATE, 0)
                                                                                 ),"i",1,"j","Test Dummy"));
        rectArrayRecord = AvroUtils.getRecord(SchemaUtils.SchemaParser.getSchema(arrSchema.getDataType().toString()), ImmutableMap.of("arr", Lists.newArrayList(
                ImmutableMap.of(CoordinateSchema.X_COORDINATE,10, CoordinateSchema.Y_COORDINATE, 20),
                ImmutableMap.of(CoordinateSchema.X_COORDINATE,20, CoordinateSchema.Y_COORDINATE, 40)), "i",1,"j","Test Dummy"));
        
        circleRecord = AvroUtils.getRecord(
                SchemaUtils.SchemaParser.getSchema(
                        new RecordSchema(TEST_NAMESPACE,
                                         "circle",
                                         Lists.newArrayList(new Field("c", CircleSchema.getSchema()),
                                                    new Field("i",new SimpleSchema(AvroConstants.PrimitiveDataType.INT)),
                                                    new Field("j",new SimpleSchema(AvroConstants.PrimitiveDataType.STRING)))).getDataType()),
                ImmutableMap.of("c",ImmutableMap.of(CircleSchema.CENTER,ImmutableMap.of(CoordinateSchema.X_COORDINATE,20, CoordinateSchema.Y_COORDINATE, 40),
                                                    CircleSchema.RADIUS,10),
                                                           "i",1,"j","Test Dummy"));
        List<Object> exRing =  Lists.newArrayList(
                ImmutableMap.of(CoordinateSchema.X_COORDINATE,0, CoordinateSchema.Y_COORDINATE, 0),
                ImmutableMap.of(CoordinateSchema.X_COORDINATE,10, CoordinateSchema.Y_COORDINATE, 0),
                ImmutableMap.of(CoordinateSchema.X_COORDINATE,10, CoordinateSchema.Y_COORDINATE, 10),
                ImmutableMap.of(CoordinateSchema.X_COORDINATE,0, CoordinateSchema.Y_COORDINATE, 10),
                ImmutableMap.of(CoordinateSchema.X_COORDINATE,0, CoordinateSchema.Y_COORDINATE, 0)
                                                 );
        List<Object> holes = Lists.newArrayList(Lists.newArrayList(ImmutableMap.of(CoordinateSchema.X_COORDINATE,0, CoordinateSchema.Y_COORDINATE, 0),
                                                                   ImmutableMap.of(CoordinateSchema.X_COORDINATE,0, CoordinateSchema.Y_COORDINATE, 5),
                                                                   ImmutableMap.of(CoordinateSchema.X_COORDINATE,5, CoordinateSchema.Y_COORDINATE, 5),
                                                                   ImmutableMap.of(CoordinateSchema.X_COORDINATE,0, CoordinateSchema.Y_COORDINATE, 0)),
                                                Lists.newArrayList(ImmutableMap.of(CoordinateSchema.X_COORDINATE,0, CoordinateSchema.Y_COORDINATE, 0),
                                                                   ImmutableMap.of(CoordinateSchema.X_COORDINATE,5, CoordinateSchema.Y_COORDINATE, 0),
                                                                   ImmutableMap.of(CoordinateSchema.X_COORDINATE,5, CoordinateSchema.Y_COORDINATE, 5),
                                                                   ImmutableMap.of(CoordinateSchema.X_COORDINATE,0, CoordinateSchema.Y_COORDINATE, 0)
                                                                   ));
        Schema polygonSchema = new RecordSchema(TEST_NAMESPACE,
                                                "polygon",
                                                Lists.newArrayList(new Field("p", PolygonSchema.getSchema()),
                                                                   new Field("i",new SimpleSchema(AvroConstants.PrimitiveDataType.INT)),
                                                                   new Field("j",new SimpleSchema(AvroConstants.PrimitiveDataType.STRING))));
        polygonRecordWithoutHoles = AvroUtils.getRecord(
                SchemaUtils.SchemaParser.getSchema(polygonSchema.getDataType().toString()),
                ImmutableMap.of("p",ImmutableMap.of(PolygonSchema.EXTERIOR_RING, exRing),
                                "i",1,"j","Test Dummy"));
        polygonRecordWithHoles = AvroUtils.getRecord(
                SchemaUtils.SchemaParser.getSchema(polygonSchema.getDataType().toString()),
                ImmutableMap.of("p",ImmutableMap.of(PolygonSchema.EXTERIOR_RING, exRing,PolygonSchema.HOLES,holes),
                                "i",1,"j","Test Dummy"));
    }
    
    
    
    @Test
    public void testPointParquetFormatterFromArray() throws Exception {
    
        Iterable<Point> pointIterable = () -> {
            try {
                return new ParquetFormatMapper<Point>(GeometryType.POINT,"arr",Lists.newArrayList("i","j")).call(Lists.newArrayList(
                        arrayRecord).iterator());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
        Point p = StreamSupport.stream(pointIterable.spliterator(), false).findFirst().get();
        Assert.equals(p, new GeometryFactory().createPoint(new Coordinate(0,0)));
        Assert.equals(((Map<String,Double>)p.getUserData()).get("i"), 1);
        Assert.equals(((Map<String,Double>)p.getUserData()).get("j"), "Test Dummy");
    }
    
    @Test
    public void testLineStringParquetFormatterFromArray() throws Exception {
        Iterable<LineString> lineStringIterable = () -> {
            try {
                return new ParquetFormatMapper<LineString>(GeometryType.LINESTRING,"arr",Lists.newArrayList("i","j")).call(Lists.newArrayList(
                        arrayRecord).iterator());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
        LineString l = StreamSupport.stream(lineStringIterable.spliterator(), false).findFirst().get();
        Assert.equals(l, new GeometryFactory().createLineString(new Coordinate[]{
                new Coordinate(0,0),
                new Coordinate(10,0),
                new Coordinate(10,10),
                new Coordinate(0,10),
                new Coordinate(0,0)}));
        Assert.equals(((Map<String,Double>)l.getUserData()).get("i"), 1);
        Assert.equals(((Map<String,Double>)l.getUserData()).get("j"), "Test Dummy");
    }
    
    @Test
    public void testPolygonParquetFormatterFromArray() throws Exception {
    
        Iterable<Polygon> iterable = () -> {
            try {
                return new ParquetFormatMapper<Polygon>(GeometryType.POLYGON,"arr",Lists.newArrayList("i","j")).call(Lists.newArrayList(
                        arrayRecord).iterator());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
        Polygon geometry = StreamSupport.stream(iterable.spliterator(), false).findFirst().get();
        Assert.equals(geometry, new GeometryFactory().createPolygon(new Coordinate[]{
                new Coordinate(0,0),
                new Coordinate(10,0),
                new Coordinate(10,10),
                new Coordinate(0,10),
                new Coordinate(0,0)}));
        Assert.equals(((Map<String,Double>)geometry.getUserData()).get("i"), 1);
        Assert.equals(((Map<String,Double>)geometry.getUserData()).get("j"), "Test Dummy");
    }
    
    @Test
    public void testRectangleParquetFormatterFromArray() throws Exception {
    
        Iterable<Polygon> iterable = () -> {
            try {
                return new ParquetFormatMapper<Polygon>(GeometryType.RECTANGLE,"arr",Lists.newArrayList("i","j")).call(Lists.newArrayList(
                        rectArrayRecord).iterator());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
        Polygon geometry = StreamSupport.stream(iterable.spliterator(), false).findFirst().get();
        Assert.equals(geometry, new GeometryFactory().createPolygon(new Coordinate[]{
                new Coordinate(10,20),
                new Coordinate(10,40),
                new Coordinate(20,40),
                new Coordinate(20,20),
                new Coordinate(10,20)}));
        Assert.equals(((Map<String,Double>)geometry.getUserData()).get("i"), 1);
        Assert.equals(((Map<String,Double>)geometry.getUserData()).get("j"), "Test Dummy");
        Assert.isTrue(geometry.isRectangle());
    }
    
    @Test
    public void testCircleFromGenericRecord(){
        Iterable<Circle> iterable = () -> {
            try {
                return new ParquetFormatMapper<Circle>(GeometryType.CIRCLE,"c",Lists.newArrayList("i","j"))
                        .call(Lists.newArrayList(circleRecord).iterator());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
        Circle geometry = StreamSupport.stream(iterable.spliterator(), false).findFirst().get();
        Assert.equals(geometry, new Circle(new GeometryFactory().createPoint(new Coordinate(20,40)),10.0));
        Assert.equals(((Map<String,Double>)geometry.getUserData()).get("i"), 1);
        Assert.equals(((Map<String,Double>)geometry.getUserData()).get("j"), "Test Dummy");
    }
    
    @Test
    public void testPolygonWithoutHolesFromGenericRecord(){
        Iterable<Polygon> iterable = () -> {
            try {
                return new ParquetFormatMapper<Polygon>(GeometryType.POLYGON,"p",Lists.newArrayList("i","j"))
                        .call(Lists.newArrayList(polygonRecordWithoutHoles).iterator());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
        Polygon geometry = StreamSupport.stream(iterable.spliterator(), false).findFirst().get();
        Assert.equals(geometry, new GeometryFactory().createPolygon(new Coordinate[]{
                new Coordinate(0,0),
                new Coordinate(10,0),
                new Coordinate(10,10),
                new Coordinate(0,10),
                new Coordinate(0,0)}));
        Assert.equals(((Map<String,Double>)geometry.getUserData()).get("i"), 1);
        Assert.equals(((Map<String,Double>)geometry.getUserData()).get("j"), "Test Dummy");
    }
    
    @Test
    public void testPolygonWithHolesFromGenericRecord(){
        Iterable<Polygon> iterable = () -> {
            try {
                return new ParquetFormatMapper<Polygon>(GeometryType.POLYGON,"p",Lists.newArrayList("i","j"))
                        .call(Lists.newArrayList(polygonRecordWithHoles).iterator());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
        GeometryFactory factory = new GeometryFactory();
        Polygon geometry = StreamSupport.stream(iterable.spliterator(), false).findFirst().get();
        Assert.equals(geometry, factory.createPolygon(factory.createLinearRing(new Coordinate[]{
                new Coordinate(0,0),
                new Coordinate(10,0),
                new Coordinate(10,10),
                new Coordinate(0,10),
                new Coordinate(0,0)}),new LinearRing[]{
                        factory.createLinearRing(new Coordinate[]{
                                new Coordinate(0,0),
                                new Coordinate(0,5),
                                new Coordinate(5,5),
                                new Coordinate(0,0)}),
                factory.createLinearRing(new Coordinate[]{
                        new Coordinate(0,0),
                        new Coordinate(5,0),
                        new Coordinate(5,5),
                        new Coordinate(0,0)
        })}));
        Assert.equals(((Map<String,Double>)geometry.getUserData()).get("i"), 1);
        Assert.equals(((Map<String,Double>)geometry.getUserData()).get("j"), "Test Dummy");
    }
}
