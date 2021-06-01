package org.apache.sedona.core.io.avro.constants;

public class AvroConstants {
    public static final String NAME = "name";
    public static final String TYPE = "type";
    public static final String RECORD = "record";
    public static final String FIELDS = "fields";
    public static final String NAMESPACE = "namespace";
    public static final String INT = "int";
    public static final String STRING = "string";
    public static final String DOUBLE = "double";
    public static final String FLOAT = "float";
    public static final String NULL = "null";
    public static final String ARRAY = "array";
    public static final String ITEMS = "items";
    public static final String NESTED = "nested";
    public static final String EMPTY_STRING = "";
    public static final String SEDONA_NAMESPACE = "org.apache.sedona.core";
    public static final String DOT = ".";
    
    public interface DataType {
        public String getType();
    }
    
    public enum PrimitiveDataType implements DataType {
        INT(AvroConstants.INT),
        DOUBLE(AvroConstants.DOUBLE),
        FLOAT(AvroConstants.FLOAT),
        STRING(AvroConstants.STRING),
        NULL(AvroConstants.NULL);
        private String type;
        
        PrimitiveDataType(String type) {
            this.type = type;
        }
        
        public String getType() {
            return type;
        }
    }
    
    public enum ComplexDataType implements DataType {
        RECORD(AvroConstants.RECORD),
        ARRAY(AvroConstants.ARRAY),
        NESTED(AvroConstants.NESTED);
        private String type;
        
        ComplexDataType(String type) {
            this.type = type;
        }
        
        @Override
        public String getType() {
            return type;
        }
    }
    
}
