package org.apache.sedona.core.io.avro.schema;

import org.apache.sedona.core.exceptions.SedonaException;
import org.apache.sedona.core.io.avro.constants.AvroConstants;
import org.json.simple.JSONObject;

public class NestedSchema extends Schema {
    private RecordSchema schema;
    
    public NestedSchema(String name, RecordSchema schema) {
        super(name);
        this.schema = schema;
    }
    
    public RecordSchema getSchema() {
        return schema;
    }
    
    @Override
    public JSONObject getAVROSchemaJson() throws SedonaException {
        JSONObject json = new JSONObject();
        json.put(AvroConstants.NAME, this.name);
        json.put(AvroConstants.TYPE, this.schema.getAVROSchemaJson());
        return json;
    }
    
    @Override
    public AvroConstants.DataType getType() {
        return AvroConstants.ComplexDataType.NESTED;
    }
}
