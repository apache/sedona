package org.apache.sedona.core.geometryObjects.schema;

import com.google.common.collect.Lists;
import org.apache.sedona.core.io.avro.constants.AvroConstants;
import org.apache.sedona.core.io.avro.schema.Field;
import org.apache.sedona.core.io.avro.schema.SimpleSchema;
import org.apache.sedona.core.io.avro.schema.RecordSchema;
import org.apache.sedona.core.utils.SedonaUtils;

public class CoordinateSchema extends RecordSchema {
    public static final String X_COORDINATE = "x";
    public static final String Y_COORDINATE = "y";
    public static final String COORDINATE = "coordinate";
    
    private CoordinateSchema() {
        super(AvroConstants.SEDONA_NAMESPACE,COORDINATE,
              Lists.newArrayList(new Field(X_COORDINATE, new SimpleSchema(AvroConstants.PrimitiveDataType.DOUBLE)),
                                 new Field(Y_COORDINATE, new SimpleSchema(AvroConstants.PrimitiveDataType.DOUBLE))));
    }
    
    private static CoordinateSchema schema;
    
    public static CoordinateSchema getSchema(){
        if(SedonaUtils.isNull(schema)){
            synchronized (CoordinateSchema.class){
                if(SedonaUtils.isNull(schema)){
                    schema = new CoordinateSchema();
                }
            }
        }
        return schema;
    }

//    public CoordinateSchema(String name, String namespace) {
//        super(name, namespace, Arrays.asList(
//                new Field(X_COORDINATE, new SimpleSchema(AvroConstants.PrimitiveDataType.DOUBLE)),
//                new Field(Y_COORDINATE, new SimpleSchema(AvroConstants.PrimitiveDataType.DOUBLE))));
//    }
}
