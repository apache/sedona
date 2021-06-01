package org.apache.sedona.core.io.avro.schema;

import org.apache.sedona.core.io.avro.constants.AvroConstants;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

public class PrimitiveSchema extends Schema{
    AvroConstants.PrimitiveDataType dataType;
    Boolean nullable;

    public PrimitiveSchema(String name, AvroConstants.PrimitiveDataType dataType, Boolean nullable) {
        super(name);
        this.dataType = dataType;
        this.nullable = nullable;
    }
    
    public AvroConstants.DataType getDataType() {
        return dataType;
    }
    
    public PrimitiveSchema(String name, AvroConstants.PrimitiveDataType dataType) {
        this(name,dataType,false);
    }

    @Override
    public JSONObject getAVROSchemaJson() {
        JSONObject json = new JSONObject();
        json.put(AvroConstants.NAME, this.name);
        if(nullable && !AvroConstants.PrimitiveDataType.NULL.equals(dataType)){
            JSONArray type = new JSONArray();
            type.add(dataType.getType());
            type.add(AvroConstants.PrimitiveDataType.NULL.getType());
            json.put(AvroConstants.TYPE, type);
        }else{
            json.put(AvroConstants.TYPE, this.dataType.getType());
        }
        return json;
    }
    
    @Override
    public AvroConstants.DataType getType() {
        return this.dataType;
    }
}
