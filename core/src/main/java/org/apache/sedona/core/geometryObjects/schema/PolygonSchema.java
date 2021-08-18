package org.apache.sedona.core.geometryObjects.schema;

import com.google.common.collect.Lists;
import org.apache.sedona.core.io.avro.constants.AvroConstants;
import org.apache.sedona.core.io.avro.schema.*;
import org.apache.sedona.core.io.avro.utils.AvroUtils;
import org.apache.sedona.core.utils.SedonaUtils;

/**
 * Polygon Schema class representing AvroSchema of Polygon Geometry
 */
public class PolygonSchema extends RecordSchema {
    public static final String POLYGON = "polygon";
    public static final String EXTERIOR_RING = "ex";
    public static final String HOLES = "holes";
    
    private PolygonSchema() {
        super(AvroConstants.SEDONA_NAMESPACE,POLYGON,
              Lists.newArrayList(new Field(EXTERIOR_RING, CoordinateArraySchema.getSchema()),
                                 new Field(HOLES, new UnionSchema(
                                         new ArraySchema(CoordinateArraySchema.getSchema()),
                                         new SimpleSchema(AvroConstants.PrimitiveDataType.NULL)))));
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
