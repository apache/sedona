package org.apache.sedona.core.io.avro.schema;

import org.apache.sedona.core.exceptions.SedonaException;
import org.apache.sedona.core.io.avro.SchemaUtils;
import org.apache.sedona.core.io.avro.constants.AvroConstants;
import org.junit.Test;
import org.locationtech.jts.util.Assert;

public class UnionSchemaTest extends BaseSchemaTest{
    @Test
    public void testUnionSchema() throws SedonaException {
        synchronized (SchemaUtils.SchemaParser.class){
            Schema schema = new UnionSchema(new SimpleSchema(AvroConstants.PrimitiveDataType.DOUBLE),new SimpleSchema(
                    AvroConstants.PrimitiveDataType.NULL));
            Assert.equals(schema.getDataType().toString(), "[\"double\",\"null\"]");
    
    
        }
    }
}
