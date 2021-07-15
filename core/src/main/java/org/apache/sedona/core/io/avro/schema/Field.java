package org.apache.sedona.core.io.avro.schema;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.sedona.core.exceptions.SedonaException;
import org.apache.sedona.core.io.avro.constants.AvroConstants;
import org.apache.spark.serializer.Serializer;
import org.json.simple.JSONObject;

import java.io.Serializable;

public class Field implements KryoSerializable, Serializable {
    private String name;
    private Schema schema;
    
    public Field(String name, Schema schema) {
        this.name = name;
        this.schema = schema;
    }
    
    public Field() {
    }
    
    public String getName() {
        return name;
    }
    
    public Schema getSchema() {
        return schema;
    }
    
    public void write(Kryo kryo, Output output) {
        output.writeString(this.name);
        kryo.writeClassAndObject(output,schema);
    }
    
    public void read(Kryo kryo, Input input) {
        this.name = input.readString();
        this.schema = (Schema) kryo.readClassAndObject(input);
    }
}
