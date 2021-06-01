package org.apache.sedona.core.geometryObjects.schema;

import org.apache.sedona.core.io.avro.constants.AvroConstants;
import org.apache.sedona.core.io.avro.schema.PrimitiveSchema;
import org.apache.sedona.core.io.avro.schema.RecordSchema;

import java.util.Arrays;

public class CoordinateSchema extends RecordSchema {
    public static final String X_COORDINATE = "x";
    public static final String Y_COORDINATE = "y";
    
    public CoordinateSchema(String name, String namespace) {
        super(name, namespace, Arrays.asList(new PrimitiveSchema(X_COORDINATE, AvroConstants.PrimitiveDataType.DOUBLE),
                                             new PrimitiveSchema(Y_COORDINATE, AvroConstants.PrimitiveDataType.DOUBLE)));
    }
}
