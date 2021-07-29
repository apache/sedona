package org.apache.sedona.core.io.avro.schema;

import avro.shaded.com.google.common.collect.Lists;
import org.apache.sedona.core.exceptions.SedonaException;
import org.apache.sedona.core.io.avro.SchemaUtils;
import org.apache.sedona.core.io.avro.constants.AvroConstants;
import org.junit.Test;
import org.locationtech.jts.util.Assert;

public class RecordSchemaTest extends BaseSchemaTest {
    @Test
    public void testRecordSchema() throws SedonaException {
        synchronized (SchemaUtils.SchemaParser.class) {
            Schema schema = new RecordSchema(TEST_NAMESPACE,
                                             TEST_NAME,
                                             Lists.newArrayList(new Field("col",
                                                                          new UnionSchema(new SimpleSchema(AvroConstants.PrimitiveDataType.INT), new SimpleSchema(
                                                                                  AvroConstants.PrimitiveDataType.NULL)))));
            String type = (String) schema.getDataType();
            schema = new ArraySchema(new UnionSchema(new SimpleSchema(String.join(".", TEST_NAMESPACE, TEST_NAME)),
                                                     new SimpleSchema(AvroConstants.PrimitiveDataType.NULL)));
            schema = new RecordSchema("x.y", "z", Lists.newArrayList(new Field("a", schema)));
            Assert.equals(schema.getDataType(), "x.y.z");
            org.apache.avro.Schema avroSchema = SchemaUtils.SchemaParser.getSchema("x.y", "z");
            Assert.equals(avroSchema.getType(), org.apache.avro.Schema.Type.RECORD);
            Assert.equals(avroSchema.getFields().size(), 1);
            Assert.equals(avroSchema.getFields().get(0).name(), "a");
            Assert.equals(avroSchema.getFields().get(0).schema().getType(), org.apache.avro.Schema.Type.ARRAY);
            Assert.equals(avroSchema.getFields()
                                    .get(0)
                                    .schema()
                                    .getElementType()
                                    .getTypes()
                                    .get(1)
                                    .getType(), org.apache.avro.Schema.Type.NULL);
            Assert.equals(avroSchema.getFields()
                                    .get(0)
                                    .schema()
                                    .getElementType()
                                    .getTypes()
                                    .get(0)
                                    .getField("col")
                                    .schema()
                                    .getTypes()
                                    .get(0)
                                    .getType(), org.apache.avro.Schema.Type.INT);
            Assert.equals(avroSchema.getFields()
                                    .get(0)
                                    .schema()
                                    .getElementType()
                                    .getTypes()
                                    .get(0)
                                    .getField("col")
                                    .schema()
                                    .getTypes()
                                    .get(1)
                                    .getType(), org.apache.avro.Schema.Type.NULL);
            
        }
        
    }
}
