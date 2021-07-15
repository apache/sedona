package org.apache.sedona.core.io.avro.schema;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.sedona.core.exceptions.SedonaException;
import org.apache.sedona.core.io.avro.SchemaUtils;
import org.apache.sedona.core.io.avro.constants.AvroConstants;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

public class SimpleSchema extends Schema {
    private String dataType;
    private Boolean nullable;
    
    public SimpleSchema(AvroConstants.PrimitiveDataType dataType, Boolean nullable) {
        this.dataType = dataType.getType();
        this.nullable = nullable;
    }
    
    public SimpleSchema() {
    }
    
    public SimpleSchema(AvroConstants.PrimitiveDataType dataType) {
        this(dataType, false);
    }
    
    public SimpleSchema(String type, Boolean nullable) throws SedonaException {
        SchemaUtils.SchemaParser.getSchema(type);
        this.dataType = type;
        this.nullable = nullable;
    }
    
    public SimpleSchema(String dataType) throws SedonaException {
        this(dataType, false);
    }
    
    @Override
    public String getDataType() {
        if (nullable && !AvroConstants.PrimitiveDataType.NULL.getType().equals(dataType)) {
            JSONArray type = new JSONArray();
            type.add(dataType);
            type.add(AvroConstants.PrimitiveDataType.NULL.getType());
            return type.toJSONString();
        }
        return dataType;
    }
    
    @Override
    public void write(Kryo kryo, Output output) {
        output.writeString(dataType);
        output.writeBoolean(nullable);
    }
    
    @Override
    public AvroConstants.SchemaType getSchemaType() {
        return AvroConstants.SchemaType.SIMPLE;
    }
    
    @Override
    public void read(Kryo kryo, Input input) {
        this.dataType = input.readString();
        this.nullable = input.readBoolean();
    }
}
