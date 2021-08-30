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
    
    /**
     * Gets Schema of given Primitive Data type
     * @param dataType
     */
    public SimpleSchema(AvroConstants.PrimitiveDataType dataType) {
        this.dataType = dataType.getType();
    }
    
    public SimpleSchema() {
        this(AvroConstants.PrimitiveDataType.NULL);
    }
    
    /**
     * Gets Schema of a predefined Datatype
     * @param type
     * @throws SedonaException
     */
    public SimpleSchema(String type) throws SedonaException {
        SchemaUtils.SchemaParser.getSchema(type);
        this.dataType = type;
    }
    
    @Override
    public String getDataType() {
        return dataType;
    }
    
    @Override
    public void write(Kryo kryo, Output output) {
        output.writeString(dataType);
    }
    
    @Override
    public AvroConstants.SchemaType getSchemaType() {
        return AvroConstants.SchemaType.SIMPLE;
    }
    
    @Override
    public void read(Kryo kryo, Input input) {
        this.dataType = input.readString();
    }
}
