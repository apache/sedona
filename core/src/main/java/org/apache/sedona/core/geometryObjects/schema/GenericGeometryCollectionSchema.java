package org.apache.sedona.core.geometryObjects.schema;

import com.google.common.collect.Lists;
import org.apache.sedona.core.io.avro.constants.AvroConstants;
import org.apache.sedona.core.io.avro.schema.*;
import org.apache.sedona.core.utils.SedonaUtils;

import static org.apache.sedona.core.io.avro.constants.AvroConstants.GEOMETRY_OBJECT;
import static org.apache.sedona.core.io.avro.constants.AvroConstants.GEOMETRY_SHAPE;

public class GenericGeometryCollectionSchema extends RecordSchema {
    public static final String GEOMETRY_COLLECTION = "GeometryCollection";
    private GenericGeometryCollectionSchema() {
        super(AvroConstants.SEDONA_NAMESPACE, GEOMETRY_COLLECTION,
              Lists.newArrayList(new Field(GEOMETRY_OBJECT, new ArraySchema(GenericGeometrySchema.getSchema())),
                                 new Field(GEOMETRY_SHAPE, new SimpleSchema(AvroConstants.PrimitiveDataType.STRING))
                                ));
    }
    
    private static GenericGeometryCollectionSchema schema;
    
    public static GenericGeometryCollectionSchema getSchema(){
        if(SedonaUtils.isNull(schema)){
            synchronized (CoordinateSchema.class){
                if(SedonaUtils.isNull(schema)){
                    schema = new GenericGeometryCollectionSchema();
                }
            }
        }
        return schema;
    }
}
