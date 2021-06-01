package org.apache.sedona.core.geometryObjects.schema;

import com.google.common.collect.Lists;
import com.sun.tools.internal.ws.wsdl.document.schema.SchemaConstants;
import org.apache.sedona.core.io.avro.constants.AvroConstants;
import org.apache.sedona.core.io.avro.schema.ArraySchema;
import org.apache.sedona.core.io.avro.schema.NestedSchema;
import org.apache.sedona.core.io.avro.schema.RecordSchema;
import org.apache.sedona.core.io.avro.schema.Schema;

import java.util.List;

public class PolygonSchema extends RecordSchema {
    public static final String EXTERIOR_RING = "ex";
    public static final String HOLES = "holes";
    public static final String HOLE = "hole";
    
    public PolygonSchema(String name, String namespace) {
        super(name, namespace, Lists.newArrayList(
                new CoordinateArraySchema(EXTERIOR_RING, String.join(AvroConstants.DOT, namespace, name)),
                new ArraySchema(HOLES, new RecordSchema(HOLE, String.join(AvroConstants.DOT, namespace, name,HOLES),
                                                        Lists.newArrayList(new CoordinateArraySchema(HOLE,
                                                                                                     String.join(
                                                                                                             AvroConstants.DOT,
                                                                                                             namespace,
                                                                                                             name,
                                                                                                             HOLES)))))));
    }
}
