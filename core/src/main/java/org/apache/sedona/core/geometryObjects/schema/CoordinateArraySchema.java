package org.apache.sedona.core.geometryObjects.schema;

import org.apache.sedona.core.io.avro.constants.AvroConstants;
import org.apache.sedona.core.io.avro.schema.ArraySchema;
import org.apache.sedona.core.io.avro.schema.Schema;

public class CoordinateArraySchema extends ArraySchema {
    public static final String COORDINATE = "coord";
    
    public CoordinateArraySchema(String name, String namespace) {
        super(name, new CoordinateSchema(COORDINATE, String.join(AvroConstants.DOT, namespace,name)));
    }
}
