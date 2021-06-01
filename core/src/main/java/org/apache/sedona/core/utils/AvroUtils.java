package org.apache.sedona.core.utils;

import org.apache.avro.Schema;

public class AvroUtils {
    public static class SchemaParser{
        public static Schema.Parser getParser(){
            return new Schema.Parser();
        }
    }
}
