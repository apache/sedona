package org.apache.sedona.core.geometryObjects.schema;

import com.google.common.collect.Lists;
import org.apache.sedona.core.io.avro.constants.AvroConstants;
import org.apache.sedona.core.io.avro.schema.*;
import org.apache.sedona.core.utils.SedonaUtils;

import static org.apache.sedona.core.io.avro.constants.AvroConstants.GEOMETRY_OBJECT;
import static org.apache.sedona.core.io.avro.constants.AvroConstants.GEOMETRY_SHAPE;

public class SpecificGeometryCollection extends RecordSchema {
    public static final String SPECIFIC_GEOMETRY_COLLECTION = "SpecificGeometryCollection";
    
    private SpecificGeometryCollection() {
        super(AvroConstants.SEDONA_NAMESPACE, SPECIFIC_GEOMETRY_COLLECTION,
              Lists.newArrayList(new Field(GEOMETRY_OBJECT, new ArraySchema(GenericGeometryUnionSchema.getSchema())),
                                 new Field(GEOMETRY_SHAPE, new SimpleSchema(AvroConstants.PrimitiveDataType.STRING))
                                ));
    }
    
    private static SpecificGeometryCollection schema;
    
    public static SpecificGeometryCollection getSchema(){
        if(SedonaUtils.isNull(schema)){
            synchronized (CoordinateSchema.class){
                if(SedonaUtils.isNull(schema)){
                    schema = new SpecificGeometryCollection();
                }
            }
        }
        return schema;
    }
}
