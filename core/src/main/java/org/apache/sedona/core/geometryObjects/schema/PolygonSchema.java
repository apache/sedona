package org.apache.sedona.core.geometryObjects.schema;

import com.google.common.collect.Lists;
import org.apache.sedona.core.io.avro.constants.AvroConstants;
import org.apache.sedona.core.io.avro.schema.ArraySchema;
import org.apache.sedona.core.io.avro.schema.Field;
import org.apache.sedona.core.io.avro.schema.RecordSchema;
import org.apache.sedona.core.io.avro.utils.AvroUtils;
import org.apache.sedona.core.utils.SedonaUtils;

public class PolygonSchema extends RecordSchema {
    public static final String POLYGON = "polygon";
    public static final String EXTERIOR_RING = "ex";
    public static final String HOLES = "holes";
    
    private PolygonSchema() {
        super(AvroConstants.SEDONA_NAMESPACE,POLYGON,
              Lists.newArrayList(new Field(EXTERIOR_RING, CoordinateArraySchema.getSchema()),
                                 new Field(HOLES, new ArraySchema(CoordinateArraySchema.getSchema()))));
    }
    
    private static PolygonSchema schema;
    
    public static PolygonSchema getSchema() {
        if (SedonaUtils.isNull(schema)) {
            synchronized (PolygonSchema.class) {
                if (SedonaUtils.isNull(schema)) {
                    schema = new PolygonSchema();
                }
            }
        }
        return schema;
    }
}
