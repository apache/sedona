package org.apache.sedona.core.io.avro.schema;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.collect.Lists;
import org.apache.sedona.core.exceptions.SedonaException;
import org.apache.sedona.core.io.avro.constants.AvroConstants;
import org.json.simple.JSONArray;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class UnionSchema extends Schema{
    List<Schema> typeSchemas;
    
    public UnionSchema(List<Schema> typeSchemas) {
        this.typeSchemas = typeSchemas;
    }
    
    public UnionSchema(Schema... typeSchemas){
        this(Arrays.stream(typeSchemas).collect(Collectors.toList()));
    }
    
    public UnionSchema() {
        this(Collections.EMPTY_LIST);
    }
    
    @Override
    public JSONArray getDataType() throws SedonaException {
        JSONArray type = new JSONArray();
        for(Schema schema: typeSchemas){
            type.add(schema.getDataType());
        }
        return type;
    }
    
    @Override
    public AvroConstants.SchemaType getSchemaType() {
        return AvroConstants.SchemaType.UNION;
    }
    
    @Override
    public void write(Kryo kryo, Output output) {
        output.writeInt(typeSchemas.size());
        for(Schema schema: typeSchemas){
            kryo.writeClassAndObject(output, schema);
        }
    }
    
    @Override
    public void read(Kryo kryo, Input input) {
        int size = input.readInt();
        this.typeSchemas = Lists.newArrayList();
        for(int i=0;i<size;i++){
            typeSchemas.add((Schema) kryo.readClassAndObject(input));
        }
    }
}
