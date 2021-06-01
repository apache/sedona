package org.apache.sedona.core.geometryObjects.schema;

import com.google.common.collect.Lists;
import org.apache.sedona.core.io.avro.schema.NestedSchema;
import org.apache.sedona.core.io.avro.schema.PrimitiveSchema;
import org.apache.sedona.core.io.avro.schema.RecordSchema;
import org.apache.sedona.core.io.avro.constants.AvroConstants;

public class CircleSchema extends RecordSchema {
    public static final String CENTER = "c";
    public static final String RADIUS = "r";
    
    public CircleSchema(String name, String namespace) {
        super(name, namespace, Lists.newArrayList(new NestedSchema(CENTER,new CoordinateSchema(CENTER, String.join(AvroConstants.DOT,namespace,name))),
                                                  new PrimitiveSchema(RADIUS, AvroConstants.PrimitiveDataType.DOUBLE)));
    }
    
    
}
