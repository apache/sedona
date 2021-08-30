package org.apache.sedona.core.geometryObjects.schema;

import com.google.common.collect.Lists;
import org.apache.sedona.core.io.avro.constants.AvroConstants;
import org.apache.sedona.core.io.avro.schema.Field;
import org.apache.sedona.core.io.avro.schema.RecordSchema;
import org.apache.sedona.core.io.avro.schema.SimpleSchema;
import org.apache.sedona.core.io.avro.schema.UnionSchema;
import org.apache.sedona.core.utils.SedonaUtils;

import static org.apache.sedona.core.io.avro.constants.AvroConstants.GEOMETRY_OBJECT;
import static org.apache.sedona.core.io.avro.constants.AvroConstants.GEOMETRY_SHAPE;

public class GenericGeometrySchema extends RecordSchema {
    public static final String GEOMETRY = "Geometry";
    
    private GenericGeometrySchema() {
        super(AvroConstants.SEDONA_NAMESPACE, GEOMETRY,
              Lists.newArrayList(new Field(GEOMETRY_OBJECT, GenericGeometryUnionSchema.getSchema()),
                                 new Field(GEOMETRY_SHAPE, new SimpleSchema(AvroConstants.PrimitiveDataType.STRING))
                                ));
    }
    
    private static GenericGeometrySchema schema;
    
    public static GenericGeometrySchema getSchema(){
        if(SedonaUtils.isNull(schema)){
            synchronized (CoordinateSchema.class){
                if(SedonaUtils.isNull(schema)){
                    schema = new GenericGeometrySchema();
                }
            }
        }
        return schema;
    }
    
}
