package org.apache.sedona.core.io.avro.schema;

import avro.shaded.com.google.common.collect.Lists;
import org.apache.sedona.core.exceptions.SedonaException;
import org.apache.sedona.core.io.avro.SchemaUtils;
import org.apache.sedona.core.io.avro.constants.AvroConstants;
import org.json.simple.JSONObject;
import org.junit.Test;
import org.locationtech.jts.util.Assert;

public class ArraySchemaTest extends BaseSchemaTest {
    @Test
    public void testArraySchema() throws SedonaException {
        synchronized (SchemaUtils.SchemaParser.class) {
            Schema schema = new ArraySchema(new SimpleSchema(AvroConstants.PrimitiveDataType.INT));
            String arraySchemaJson = (String) schema.getDataType().toString();
            Assert.equals("{\"type\":\"array\",\"items\":\"int\"}",arraySchemaJson);
        }
    }
    
    @Test(expected = SedonaException.class)
    public void testInvalidArraySchema() throws SedonaException {
        synchronized (SchemaUtils.SchemaParser.class){
            Schema schema = new ArraySchema(new UnionSchema(
                    new SimpleSchema(String.join(".",TEST_NAMESPACE,TEST_NAME)),
                    new SimpleSchema(AvroConstants.PrimitiveDataType.NULL)
            ));
        }
    }
    
    @Test
    public void testValidArraySchema() throws SedonaException {
        synchronized (SchemaUtils.SchemaParser.class){
            Schema schema = new RecordSchema(TEST_NAMESPACE, TEST_NAME, Lists.newArrayList(new Field("col", new SimpleSchema(
                    AvroConstants.PrimitiveDataType.INT))));
            String type = (String)schema.getDataType();
            schema = new ArraySchema(
                    new UnionSchema(
                            new SimpleSchema(String.join(".",TEST_NAMESPACE,TEST_NAME)),
                            new SimpleSchema(AvroConstants.PrimitiveDataType.NULL)));
            Assert.equals("{\"type\":\"array\",\"items\":[\""+type+"\",\"null\"]}",
                          ((JSONObject)schema.getDataType()).toJSONString());
        }
    }
}
