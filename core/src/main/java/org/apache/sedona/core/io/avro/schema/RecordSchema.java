package org.apache.sedona.core.io.avro.schema;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.sedona.core.exceptions.SedonaException;
import org.apache.sedona.core.geometryObjects.schema.CircleSchema;
import org.apache.sedona.core.geometryObjects.schema.CoordinateSchema;
import org.apache.sedona.core.geometryObjects.schema.PolygonSchema;
import org.apache.sedona.core.io.avro.SchemaUtils;
import org.apache.sedona.core.io.avro.constants.AvroConstants;
import org.apache.sedona.core.io.avro.utils.AvroUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import static org.apache.parquet.cli.util.Expressions.filterSchema;

import java.util.*;

public class RecordSchema extends Schema {
    private String name;
    private String namespace;
    private List<Field> fields;
    
    public RecordSchema(String namespace, String name, List<Field> fields) {
        this.name = name;
        this.namespace = namespace;
        this.fields = fields;
    }
    
    public RecordSchema() {
    }
    
    public List<Field> getFields() {
        return fields;
    }
    
    @Override
    public String getDataType() throws SedonaException {
        JSONObject json = new JSONObject();
        json.put(AvroConstants.NAME, this.name);
        json.put(AvroConstants.TYPE, AvroConstants.RECORD);
        json.put(AvroConstants.NAMESPACE, this.namespace);
        JSONArray fields = new JSONArray();
        for (Field field : this.fields) {
            JSONObject fieldJson = new JSONObject();
            fieldJson.put(AvroConstants.NAME, field.getName());
            fieldJson.put(AvroConstants.TYPE, field.getSchema().getDataType());
            fields.add(fieldJson);
        }
        json.put(AvroConstants.FIELDS, fields);
        return SchemaUtils.SchemaParser.parseJson(namespace,name,json);
    }
    
    @Override
    public void write(Kryo kryo, Output output) {
        output.writeString(name);
        output.writeString(namespace);
        output.writeInt(fields.size());
        for(Field field:fields){
            kryo.writeClassAndObject(output,field);
        }
        
    }
    
    @Override
    public AvroConstants.SchemaType getSchemaType() {
        return AvroConstants.SchemaType.RECORD;
    }
    
    @Override
    public void read(Kryo kryo, Input input) {
        this.name = input.readString();
        this.namespace = input.readString();
        this.fields = new ArrayList<>();
        int size = input.readInt();
        for(int i=0;i<size;i++){
            fields.add((Field) kryo.readClassAndObject(input));
        }
    }
}
