package org.apache.sedona.core.io.avro.schema;

import org.apache.sedona.core.exceptions.SedonaException;
import org.apache.sedona.core.io.avro.constants.AvroConstants;
import org.json.simple.JSONObject;

public class ArraySchema extends Schema{
    private Schema items;
    
    public ArraySchema(String name, Schema items) {
        super(name);
        this.items = items;
    }
    
    public Schema getItems() {
        return items;
    }
    
    @Override
    public JSONObject getAVROSchemaJson() throws SedonaException {
        JSONObject json = new JSONObject();
        json.put(AvroConstants.NAME, this.name);
        JSONObject type  = new JSONObject();
        type.put(AvroConstants.TYPE, AvroConstants.ARRAY);
        if(items.getType() instanceof AvroConstants.PrimitiveDataType){
            type.put(AvroConstants.ITEMS, items.getType().getType());
        }else{
            type.put(AvroConstants.ITEMS, items.getAVROSchemaJson());
        }
        json.put(AvroConstants.TYPE, type);
        return json;
    }
    
    @Override
    public AvroConstants.DataType getType() {
        return AvroConstants.ComplexDataType.ARRAY;
    }
}
