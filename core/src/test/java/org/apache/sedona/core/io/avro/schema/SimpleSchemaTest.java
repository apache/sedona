package org.apache.sedona.core.io.avro.schema;

import avro.shaded.com.google.common.collect.Lists;
import org.apache.sedona.core.exceptions.SedonaException;
import org.apache.sedona.core.io.avro.SchemaUtils;
import org.apache.sedona.core.io.avro.constants.AvroConstants;
import org.junit.Test;
import org.locationtech.jts.util.Assert;

public class SimpleSchemaTest extends BaseSchemaTest {
    
    @Test
    public void testPrimitiveSchema() throws SedonaException {
        synchronized (SchemaUtils.SchemaParser.class) {
            Schema schema = new SimpleSchema(AvroConstants.PrimitiveDataType.STRING);
            Assert.equals(schema.getDataType().toString(),"string");
            schema = new SimpleSchema(AvroConstants.PrimitiveDataType.DOUBLE);
            Assert.equals(schema.getDataType(),"double");
            schema = new SimpleSchema(AvroConstants.PrimitiveDataType.INT);
            Assert.equals(schema.getDataType().toString(),"int");
            schema = new SimpleSchema(AvroConstants.PrimitiveDataType.FLOAT);
            Assert.equals(schema.getDataType(),"float");
            schema = new SimpleSchema(AvroConstants.PrimitiveDataType.NULL);
            Assert.equals(schema.getDataType(),"null");
        }
    }
    
    @Test(expected = SedonaException.class)
    public void testInvalidComplexSchema() throws SedonaException {
        synchronized (SchemaUtils.SchemaParser.class) {
            Schema schema = new SimpleSchema(String.join(".",TEST_NAMESPACE,TEST_NAME));
        }
    }
    
    @Test
    public void testValidComplexSchema() throws SedonaException {
        synchronized (SchemaUtils.SchemaParser.class) {
            Schema schema = new RecordSchema(TEST_NAMESPACE, TEST_NAME, Lists.newArrayList(new Field("col",new SimpleSchema(
                    AvroConstants.PrimitiveDataType.INT))));
            String type = (String)schema.getDataType();
            schema = new SimpleSchema(String.join(".",TEST_NAMESPACE,TEST_NAME));
            Assert.equals(schema.getDataType(),type);
        }
    }
}
