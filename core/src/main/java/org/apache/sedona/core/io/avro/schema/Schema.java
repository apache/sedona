package org.apache.sedona.core.io.avro.schema;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.sedona.core.exceptions.SedonaException;
import org.apache.sedona.core.io.avro.SchemaUtils;
import org.apache.sedona.core.io.avro.constants.AvroConstants;
import org.json.simple.JSONObject;

import java.io.Serializable;

public abstract class Schema implements KryoSerializable, Serializable {
    public abstract Object getDataType() throws SedonaException;
    
    public abstract AvroConstants.SchemaType getSchemaType();
    
    public static Schema readSchema(Kryo kryo, Input input){
        Schema schema;
        switch (AvroConstants.SchemaType.getSchema(input.readInt())){
            case RECORD:
                schema = new RecordSchema();
                break;
            case ARRAY:
                schema = new ArraySchema();
                break;
            case SIMPLE:
                schema = new SimpleSchema();
                break;
            default:
                return null;
        }
        schema.read(kryo,input);
        return schema;
    }
}
