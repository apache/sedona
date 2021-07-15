package org.apache.sedona.core.geometryObjects.schema;

import com.google.common.collect.Lists;
import org.apache.sedona.core.io.avro.schema.Field;
import org.apache.sedona.core.io.avro.schema.SimpleSchema;
import org.apache.sedona.core.io.avro.schema.RecordSchema;
import org.apache.sedona.core.io.avro.constants.AvroConstants;
import org.apache.sedona.core.utils.SedonaUtils;

public class CircleSchema extends RecordSchema {
    public static final String CENTER = "c";
    public static final String RADIUS = "r";
    public static final String CIRCLE = "circle";
    
    private CircleSchema() {
        super(AvroConstants.SEDONA_NAMESPACE,CIRCLE,
              Lists.newArrayList(new Field(CENTER, CoordinateSchema.getSchema()),
                                 new Field(RADIUS, new SimpleSchema(AvroConstants.PrimitiveDataType.DOUBLE))));
    }
    
    private static CircleSchema schema;
    
    public static CircleSchema getSchema(){
        if(SedonaUtils.isNull(schema)){
            synchronized (CircleSchema.class){
                if(SedonaUtils.isNull(schema)){
                    schema = new CircleSchema();
                }
            }
        }
        return schema;
    }
    
    
    
}
