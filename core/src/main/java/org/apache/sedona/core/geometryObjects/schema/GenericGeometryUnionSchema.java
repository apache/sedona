package org.apache.sedona.core.geometryObjects.schema;

import org.apache.sedona.core.io.avro.schema.UnionSchema;
import org.apache.sedona.core.utils.SedonaUtils;

public class GenericGeometryUnionSchema extends UnionSchema {
    private GenericGeometryUnionSchema() {
        super(CoordinateSchema.getSchema(),
              CircleSchema.getSchema(),
              CoordinateArraySchema.getSchema(),
              PolygonSchema.getSchema()
             );
    }
    
    private static GenericGeometryUnionSchema schema;
    
    public static GenericGeometryUnionSchema getSchema(){
        if(SedonaUtils.isNull(schema)){
            synchronized (CoordinateSchema.class){
                if(SedonaUtils.isNull(schema)){
                    schema = new GenericGeometryUnionSchema();
                }
            }
        }
        return schema;
    }
}
