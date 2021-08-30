package org.apache.sedona.core.geometryObjects.schema;

import org.apache.sedona.core.io.avro.schema.UnionSchema;
import org.apache.sedona.core.utils.SedonaUtils;

public class SedonaParquetFileGeometrySchema extends UnionSchema {
    private SedonaParquetFileGeometrySchema() {
        super(GenericGeometrySchema.getSchema(),
              GenericGeometryCollectionSchema.getSchema(),
              SpecificGeometryCollection.getSchema());
    }
    
    private static SedonaParquetFileGeometrySchema schema;
    
    public static SedonaParquetFileGeometrySchema getSchema(){
        if(SedonaUtils.isNull(schema)){
            synchronized (CoordinateSchema.class){
                if(SedonaUtils.isNull(schema)){
                    schema = new SedonaParquetFileGeometrySchema();
                }
            }
        }
        return schema;
    }
}
