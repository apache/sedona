package org.apache.sedona.core.io.avro.schema;

import org.apache.sedona.core.exceptions.SedonaException;
import org.apache.sedona.core.io.avro.constants.AvroConstants;
import org.apache.sedona.core.utils.AvroUtils;
import org.json.simple.JSONObject;

public abstract class Schema {
    protected String name;
    
    public Schema(String name) {
        this.name = name;
    }
    
    public String getName() {
        return name;
    }
    
    public org.apache.avro.Schema getAvroSchema() throws SedonaException {
        return AvroUtils.SchemaParser.getParser().parse(this.getAVROSchemaJson().toJSONString());
    }
    public abstract JSONObject getAVROSchemaJson() throws SedonaException;
    
    public abstract AvroConstants.DataType getType();
    
    
}
