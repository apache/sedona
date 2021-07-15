package org.apache.sedona.core.io.avro.schema;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.sedona.core.exceptions.SedonaException;
import org.apache.sedona.core.io.avro.constants.AvroConstants;
import org.json.simple.JSONObject;

public class ArraySchema extends Schema{
    private Schema items;
    
    public ArraySchema(Schema items) {
        this.items = items;
    }
    
    public ArraySchema() {
    }
    
    public Schema getItems() {
        return items;
    }
    
    @Override
    public JSONObject getDataType() throws SedonaException {
        JSONObject json  = new JSONObject();
        json.put(AvroConstants.TYPE, AvroConstants.ARRAY);
        json.put(AvroConstants.ITEMS, items.getDataType());
        return json;
    }
    
    @Override
    public void write(Kryo kryo, Output output) {
        kryo.writeClassAndObject(output,items);
    }
    
    @Override
    public AvroConstants.SchemaType getSchemaType() {
        return AvroConstants.SchemaType.ARRAY;
    }
    
    @Override
    public void read(Kryo kryo, Input input) {
        items = (Schema) kryo.readClassAndObject(input);
    }
}
