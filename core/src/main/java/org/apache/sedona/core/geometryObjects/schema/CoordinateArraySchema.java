package org.apache.sedona.core.geometryObjects.schema;

import org.apache.sedona.core.io.avro.schema.ArraySchema;
import org.apache.sedona.core.utils.SedonaUtils;

public class CoordinateArraySchema extends ArraySchema {
    private CoordinateArraySchema() {
        super(CoordinateSchema.getSchema());
    }
    
    private static CoordinateArraySchema schema;
    
    public static CoordinateArraySchema getSchema(){
        if(SedonaUtils.isNull(schema)){
            synchronized (CoordinateArraySchema.class){
                if(SedonaUtils.isNull(schema)){
                    schema = new CoordinateArraySchema();
                }
            }
        }
        return schema;
    }
}
