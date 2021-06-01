package org.apache.sedona.core.io.avro.schema;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.cli.util.Schemas;
import org.apache.sedona.core.exceptions.SedonaException;
import org.apache.sedona.core.geometryObjects.schema.CircleSchema;
import org.apache.sedona.core.geometryObjects.schema.CoordinateArraySchema;
import org.apache.sedona.core.geometryObjects.schema.CoordinateSchema;
import org.apache.sedona.core.geometryObjects.schema.PolygonSchema;
import org.apache.sedona.core.io.avro.constants.AvroConstants;
import org.apache.sedona.core.io.avro.utils.AvroUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import static org.apache.parquet.cli.util.Expressions.filterSchema;

import java.util.*;

public class RecordSchema extends Schema {
    private String namespace;
    private List<Schema> fields;
    
    public RecordSchema(String name, String namespace, List<Schema> fields) {
        super(name);
        this.namespace = namespace;
        this.fields = fields;
    }
    
    public List<Schema> getFields() {
        return fields;
    }
    
    @Override
    public JSONObject getAVROSchemaJson() throws SedonaException {
        JSONObject json = new JSONObject();
        json.put(AvroConstants.NAME, this.name);
        json.put(AvroConstants.TYPE, AvroConstants.RECORD);
        json.put(AvroConstants.NAMESPACE, this.namespace);
        JSONArray fields = new JSONArray();
        for (Schema field : this.fields) {
            if (AvroConstants.ComplexDataType.RECORD.equals(field.getType())) {
                throw new SedonaException(
                        "Record Schema within another RecordSchema is not allowed create a nested schema instead");
            }
            fields.add(field.getAVROSchemaJson());
        }
        json.put(AvroConstants.FIELDS, fields);
        return json;
    }
    
    @Override
    public AvroConstants.DataType getType() {
        return AvroConstants.ComplexDataType.RECORD;
    }
    
    public static void main(String[] args) throws SedonaException {
        Schema schema = new RecordSchema("rishi", "org.rishi", Lists.newArrayList(
                new PrimitiveSchema("col1", AvroConstants.PrimitiveDataType.INT, true),
                new PrimitiveSchema("col2", AvroConstants.PrimitiveDataType.STRING, false),
                new PrimitiveSchema("col3", AvroConstants.PrimitiveDataType.FLOAT, true),
                new CoordinateArraySchema("col4","ns3"),
                new NestedSchema("col5", new CoordinateSchema("col5", "org.rishi")),
                new NestedSchema("rishi5", new RecordSchema("rishi2", "org.rishi", Lists.newArrayList(
                        new PrimitiveSchema("col2", AvroConstants.PrimitiveDataType.DOUBLE),
                        new PrimitiveSchema("col4", AvroConstants.PrimitiveDataType.DOUBLE))))));
        
        Schema schema1 = new RecordSchema("rishi1", "org.rishi", Lists.newArrayList(
                new PrimitiveSchema("col1", AvroConstants.PrimitiveDataType.INT, true),
                new PrimitiveSchema("col2", AvroConstants.PrimitiveDataType.STRING, false),
                new PrimitiveSchema("col3", AvroConstants.PrimitiveDataType.FLOAT, true),
                new NestedSchema("circle", new CircleSchema("c", "org.rishi"))));
        System.out.println(schema.getAVROSchemaJson().toJSONString());
        System.out.println(schema1.getAVROSchemaJson().toJSONString());
        org.apache.avro.Schema s1 = schema.getAvroSchema();
        org.apache.avro.Schema s2 = schema1.getAvroSchema();
        s1 = filterSchema(s1, Arrays.asList("col1", "col2","col5.x"));
//        s2 = filterSchema(s2,Arrays.asList("c.r"));
//        GenericRecord genericRecord = new GenericData.Record(s2);
//        GenericRecord circle = new GenericData.Record(s2.getField("c").schema());
//        GenericRecord center = new GenericData.Record(s2.getField("c").schema().getField("c").schema());
//        center.put("x", 1);
//        center.put("y", 2);
//        circle.put("c", center);
//        circle.put("r", 1);
//        genericRecord.put("col1", 1);
//        genericRecord.put("col2", "Hi");
//        genericRecord.put("col3", 1.0);
//        genericRecord.put("c", circle);
//        System.out.println(genericRecord);
        Map<String, Object> x = ImmutableMap.of("col1", 1, "col2", "Hi", "col3", 1.0, "circle",
                                                ImmutableMap.of("c", ImmutableMap.of("x", 1, "y", 2), "r",
                                                                5));
        Map<String, Object> y = new ImmutableMap.Builder<String, Object>().put("col1", 1)
                .put("col2", "hihi")
                .put("col3", 2.0)
                .put("col4",
                     ImmutableSet.of(ImmutableMap.of("x", 1, "y", 2), ImmutableMap.of("x", 1, "y", 4)))
                .put("col5", ImmutableMap.of("x", 4, "y", 5))
                .put("rishi5", ImmutableMap.of("col2", 5.78978, "col4", 4898989.3947))
                .build();
        GenericRecord genericRecord = (GenericRecord) AvroUtils.getRecord(s2, x);
        GenericRecord genericRecord1 = (GenericRecord) AvroUtils.getRecord(s1, y);
        System.out.println(genericRecord);
        System.out.println(genericRecord1);
        PolygonSchema schema3 = new PolygonSchema("col","ns");
        System.out.println(schema3.getAVROSchemaJson().toJSONString());
        org.apache.avro.Schema s4 = schema3.getAvroSchema();
        System.out.println(s4);
    }
}
